"""
AiGlass+ Lead Automation Server
================================
FastAPI server that:
- Receives Meta Lead Form webhooks
- Routes leads: Auto Glass → instant quote via SMS | Flat Glass → calendar booking + SMS
- Logs all leads to Google Sheets (raw + CRM tabs)
- Sends Twilio SMS confirmations and follow-ups
- Books Google Calendar events for flat glass estimates
- Dead-letter queue for incomplete leads
- Health check endpoint
"""

import os
import json
import hmac
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, PlainTextResponse
from twilio.rest import Client as TwilioClient
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
from apscheduler.schedulers.background import BackgroundScheduler
from openai import OpenAI

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── Configuration ───────────────────────────────────────────────────────────
TWILIO_ACCOUNT_SID   = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN    = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM_NUMBER   = os.getenv("TWILIO_FROM_NUMBER", "")
OWNER_PHONE          = os.getenv("OWNER_PHONE", "")
CALENDAR_ID          = os.getenv("CALENDAR_ID", "tsuntsun21@gmail.com")
BOOKING_URL          = os.getenv("BOOKING_URL", "aiglassplusnw.com/book")
GOOGLE_SHEETS_ID     = os.getenv("GOOGLE_SHEETS_ID", "103aV_WElwDG80L-UyPMS9Bj01B914mJ_YQ8HvT892o8")
META_APP_SECRET      = os.getenv("META_APP_SECRET", "")           # Set after Meta app created
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "/app/service_account.json")
OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")

# Auto glass service types (Path B)
AUTO_GLASS_TYPES = {"auto glass", "windshield", "auto", "car", "vehicle", "truck", "suv"}

# Flat glass service types (Path A)
FLAT_GLASS_TYPES = {
    "residential window", "residential", "commercial glass", "commercial",
    "shower door", "shower", "skylight", "skylight installation",
    "mirror installation", "mirror", "emergency glass repair", "emergency"
}

# ─── Clients ─────────────────────────────────────────────────────────────────
twilio = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

SCOPES = ["https://www.googleapis.com/auth/calendar",
          "https://www.googleapis.com/auth/spreadsheets"]

def get_google_credentials():
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

def get_calendar_service():
    return build("calendar", "v3", credentials=get_google_credentials())

def get_sheets_client():
    return gspread.authorize(get_google_credentials())

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ─── FastAPI App ──────────────────────────────────────────────────────────────
app = FastAPI(title="AiGlass+ Lead Automation", version="1.0.0")

# ─── Health Check ─────────────────────────────────────────────────────────────
@app.get("/_health")
async def health_check():
    status = {"server": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}
    # Check calendar
    try:
        svc = get_calendar_service()
        svc.calendars().get(calendarId=CALENDAR_ID).execute()
        status["google_calendar"] = "ok"
    except Exception as e:
        status["google_calendar"] = f"error: {e}"
    # Check sheets
    try:
        gc = get_sheets_client()
        gc.open_by_key(GOOGLE_SHEETS_ID)
        status["google_sheets"] = "ok"
    except Exception as e:
        status["google_sheets"] = f"error: {e}"
    # Check Twilio
    try:
        twilio.api.accounts(TWILIO_ACCOUNT_SID).fetch()
        status["twilio"] = "ok"
    except Exception as e:
        status["twilio"] = f"error: {e}"
    return JSONResponse(status)

# ─── Meta Webhook Verification ────────────────────────────────────────────────
@app.get("/webhook")
async def verify_webhook(request: Request):
    """Meta webhook verification challenge"""
    params = dict(request.query_params)
    mode      = params.get("hub.mode")
    token     = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")
    verify_token = os.getenv("META_VERIFY_TOKEN", "aiglass_webhook_2024")
    if mode == "subscribe" and token == verify_token:
        logger.info("Webhook verified by Meta")
        return PlainTextResponse(challenge)
    raise HTTPException(status_code=403, detail="Verification failed")

# ─── Meta Webhook Receiver ────────────────────────────────────────────────────
@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    """Receive and process Meta Lead Form submissions"""
    body = await request.body()

    # Signature verification (when META_APP_SECRET is set)
    if META_APP_SECRET:
        sig_header = request.headers.get("X-Hub-Signature-256", "")
        expected = "sha256=" + hmac.new(
            META_APP_SECRET.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig_header, expected):
            raise HTTPException(status_code=403, detail="Invalid signature")

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    logger.info(f"Webhook received: {json.dumps(data)[:500]}")

    # Parse Meta lead form payload
    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            if change.get("field") == "leadgen":
                lead_value = change.get("value", {})
                background_tasks.add_task(process_lead, lead_value)

    return JSONResponse({"status": "received"})

# ─── Lead Processing ──────────────────────────────────────────────────────────
async def process_lead(lead_data: dict):
    """Main lead processing pipeline"""
    try:
        lead = extract_lead_fields(lead_data)
        logger.info(f"Processing lead: {lead}")

        # Validate required fields
        if not lead.get("name") or not lead.get("phone"):
            await handle_dead_letter(lead, "Missing required fields: name or phone")
            return

        # Log to raw sheet tab
        log_to_sheets(lead, tab="Raw Leads")

        # Route based on service type
        service = lead.get("service_type", "").lower().strip()
        if any(t in service for t in AUTO_GLASS_TYPES):
            await handle_auto_glass_lead(lead)
        elif any(t in service for t in FLAT_GLASS_TYPES):
            await handle_flat_glass_lead(lead)
        else:
            # Unknown service type — notify owner and log
            await handle_dead_letter(lead, f"Unknown service type: '{service}'")

    except Exception as e:
        logger.error(f"Error processing lead: {e}", exc_info=True)
        try:
            send_sms(OWNER_PHONE,
                f"⚠️ AiGlass+ ERROR processing lead. Check server logs.\n"
                f"Lead data: {json.dumps(lead_data)[:200]}")
        except Exception:
            pass

def extract_lead_fields(lead_data: dict) -> dict:
    """Extract fields from Meta lead form payload"""
    lead = {
        "lead_id": lead_data.get("leadgen_id", ""),
        "form_id": lead_data.get("form_id", ""),
        "page_id": lead_data.get("page_id", ""),
        "created_time": lead_data.get("created_time", ""),
        "name": "",
        "phone": "",
        "email": "",
        "service_type": "",
        "address": "",
        "vin": "",
        "vehicle_year": "",
        "vehicle_make": "",
        "vehicle_model": "",
        "additional_details": "",
    }

    # Parse field_data array from Meta
    for field in lead_data.get("field_data", []):
        name = field.get("name", "").lower()
        values = field.get("values", [])
        value = values[0] if values else ""

        if "full_name" in name or name == "name":
            lead["name"] = value
        elif "phone" in name:
            lead["phone"] = value
        elif "email" in name:
            lead["email"] = value
        elif "service" in name:
            lead["service_type"] = value
        elif "address" in name or "street" in name or "location" in name:
            lead["address"] = value
        elif "vin" in name:
            lead["vin"] = value
        elif "year" in name:
            lead["vehicle_year"] = value
        elif "make" in name:
            lead["vehicle_make"] = value
        elif "model" in name:
            lead["vehicle_model"] = value
        elif "detail" in name or "note" in name or "additional" in name:
            lead["additional_details"] = value

    return lead

# ─── Path A: Flat Glass → Calendar Booking ───────────────────────────────────
async def handle_flat_glass_lead(lead: dict):
    """Book a free estimate on the calendar and notify lead + owner"""
    logger.info(f"Path A (Flat Glass): {lead['name']}")

    # Find next available slot (next business day, morning preferred)
    slot = find_next_available_slot()
    slot_str = slot.strftime("%A, %B %-d at %-I:%M %p")

    # Create calendar event
    event_title = f"{lead['service_type']} Estimate - {lead['name']}"
    address = lead.get("address") or "Address TBD - confirm with client"
    event = create_calendar_event(
        title=event_title,
        start=slot,
        end=slot + timedelta(hours=1),
        location=address,
        description=(
            f"Client: {lead['name']}\n"
            f"Phone: {lead['phone']}\n"
            f"Email: {lead['email']}\n"
            f"Service: {lead['service_type']}\n"
            f"Notes: {lead.get('additional_details', 'None')}\n"
            f"Lead ID: {lead.get('lead_id', 'N/A')}"
        )
    )

    # SMS to lead
    lead_msg = (
        f"Hi {lead['name'].split()[0]}! This is AiGlass+ (206) 775-1567.\n"
        f"Your FREE {lead['service_type']} estimate is booked for "
        f"{slot_str} Pacific time.\n"
        f"We'll come to you!\n\n"
        f"Want to pick your own time? Book online anytime:\n"
        f"{BOOKING_URL}\n\n"
        f"Questions? Call or text (206) 775-1567."
    )
    send_sms(lead["phone"], lead_msg)

    # SMS to owner
    owner_msg = (
        f"📋 NEW FLAT GLASS LEAD BOOKED\n"
        f"Name: {lead['name']}\n"
        f"Phone: {lead['phone']}\n"
        f"Service: {lead['service_type']}\n"
        f"Slot: {slot_str}\n"
        f"Address: {address}"
    )
    send_sms(OWNER_PHONE, owner_msg)

    # Update CRM
    update_crm(lead, status="Booked", booked_slot=slot_str, sms_sent=True)
    logger.info(f"Flat glass lead booked: {event.get('id')}")

# ─── Path B: Auto Glass → Instant Quote ──────────────────────────────────────
async def handle_auto_glass_lead(lead: dict):
    """Generate instant auto glass quote and send via SMS"""
    logger.info(f"Path B (Auto Glass): {lead['name']}")

    vehicle = build_vehicle_string(lead)
    quote = generate_auto_glass_quote(vehicle, lead.get("additional_details", ""))

    # SMS to lead
    lead_msg = (
        f"Hi {lead['name'].split()[0]}! This is AiGlass+ (206) 775-1567.\n"
        f"Here's your instant quote for {vehicle}:\n\n"
        f"{quote}\n\n"
        f"Mobile service - we come to you!\n"
        f"Book your appointment online anytime:\n"
        f"{BOOKING_URL}"
    )
    send_sms(lead["phone"], lead_msg)

    # SMS to owner
    owner_msg = (
        f"🚗 NEW AUTO GLASS LEAD\n"
        f"Name: {lead['name']}\n"
        f"Phone: {lead['phone']}\n"
        f"Vehicle: {vehicle}\n"
        f"Quote sent via SMS."
    )
    send_sms(OWNER_PHONE, owner_msg)

    # Update CRM
    update_crm(lead, status="Quote Sent", quote_sent=True)
    logger.info(f"Auto glass quote sent to {lead['phone']}")

def build_vehicle_string(lead: dict) -> str:
    """Build a readable vehicle description from lead fields"""
    parts = []
    if lead.get("vehicle_year"):
        parts.append(lead["vehicle_year"])
    if lead.get("vehicle_make"):
        parts.append(lead["vehicle_make"])
    if lead.get("vehicle_model"):
        parts.append(lead["vehicle_model"])
    if lead.get("vin"):
        return f"VIN {lead['vin']}"
    return " ".join(parts) if parts else "your vehicle"

def generate_auto_glass_quote(vehicle: str, details: str = "") -> str:
    """Generate auto glass quote using pricing tiers + optional AI enhancement"""
    # Base pricing tiers (industry standard for Seattle market)
    base_quote = (
        "🔹 Windshield Replacement: $299–$450\n"
        "🔹 Door Glass: $140–$220\n"
        "🔹 Rear Window: $180–$320\n"
        "🔹 ADAS Calibration (if equipped): included\n"
        "🔹 Mobile service: FREE\n"
        "🔹 Lifetime warranty on all installs"
    )

    # If OpenAI is configured and we have vehicle details, enhance the quote
    if openai_client and vehicle != "your vehicle":
        try:
            prompt = (
                f"You are an auto glass pricing assistant for AiGlass+ in Seattle, WA. "
                f"Generate a brief, friendly SMS quote for a {vehicle}. "
                f"Include windshield replacement price range ($299-$450 typical), "
                f"mention ADAS calibration if it's a 2015+ vehicle, "
                f"and mobile service is free. Keep it under 160 characters total for the quote section. "
                f"Additional details from customer: {details or 'none'}"
            )
            response = openai_client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=150
            )
            ai_quote = response.choices[0].message.content.strip()
            return ai_quote
        except Exception as e:
            logger.warning(f"AI quote generation failed, using base pricing: {e}")

    return base_quote

# ─── Dead Letter Queue ────────────────────────────────────────────────────────
async def handle_dead_letter(lead: dict, reason: str):
    """Handle leads with missing/invalid data - alert owner immediately"""
    logger.warning(f"Dead letter lead: {reason} | Data: {lead}")
    msg = (
        f"⚠️ INCOMPLETE LEAD - ACTION NEEDED\n"
        f"Reason: {reason}\n"
        f"Name: {lead.get('name', 'Unknown')}\n"
        f"Phone: {lead.get('phone', 'Unknown')}\n"
        f"Email: {lead.get('email', 'Unknown')}\n"
        f"Service: {lead.get('service_type', 'Unknown')}"
    )
    send_sms(OWNER_PHONE, msg)
    log_to_sheets(lead, tab="Dead Letters", extra={"reason": reason})

# ─── Google Calendar ──────────────────────────────────────────────────────────
def find_next_available_slot() -> datetime:
    """Find next available 1-hour morning slot on the calendar"""
    svc = get_calendar_service()
    tz = timezone(timedelta(hours=-7))  # Pacific Daylight Time

    # Start looking from tomorrow morning
    search_start = datetime.now(tz).replace(hour=8, minute=0, second=0, microsecond=0)
    if datetime.now(tz).hour >= 8:
        search_start += timedelta(days=1)

    # Skip weekends
    while search_start.weekday() >= 5:
        search_start += timedelta(days=1)

    # Check up to 7 days out
    for day_offset in range(7):
        candidate_day = search_start + timedelta(days=day_offset)
        if candidate_day.weekday() >= 5:
            continue

        # Try morning slots: 8am, 9am, 10am, 11am
        for hour in [8, 9, 10, 11]:
            slot = candidate_day.replace(hour=hour, minute=0, second=0)
            slot_end = slot + timedelta(hours=1)

            # Check for conflicts
            events_result = svc.events().list(
                calendarId=CALENDAR_ID,
                timeMin=slot.isoformat(),
                timeMax=slot_end.isoformat(),
                singleEvents=True
            ).execute()

            if not events_result.get("items"):
                return slot

    # Fallback: return next business day at 9am if all slots busy
    fallback = search_start + timedelta(days=1)
    while fallback.weekday() >= 5:
        fallback += timedelta(days=1)
    return fallback.replace(hour=9, minute=0)

def create_calendar_event(title: str, start: datetime, end: datetime,
                           location: str, description: str) -> dict:
    """Create a Google Calendar event"""
    svc = get_calendar_service()
    event = {
        "summary": title,
        "location": location,
        "description": description,
        "start": {"dateTime": start.isoformat(), "timeZone": "America/Los_Angeles"},
        "end":   {"dateTime": end.isoformat(),   "timeZone": "America/Los_Angeles"},
        "reminders": {
            "useDefault": False,
            "overrides": [
                {"method": "popup",  "minutes": 60},
                {"method": "popup",  "minutes": 15},
            ]
        }
    }
    return svc.events().insert(calendarId=CALENDAR_ID, body=event).execute()

# ─── Twilio SMS ───────────────────────────────────────────────────────────────
def send_sms(to: str, body: str):
    """Send SMS via Twilio"""
    try:
        msg = twilio.messages.create(
            body=body,
            from_=TWILIO_FROM_NUMBER,
            to=to
        )
        logger.info(f"SMS sent to {to}: SID={msg.sid}")
    except Exception as e:
        logger.error(f"SMS failed to {to}: {e}")

# ─── Google Sheets CRM ────────────────────────────────────────────────────────
def log_to_sheets(lead: dict, tab: str = "Raw Leads", extra: dict = None):
    """Log lead to Google Sheets"""
    try:
        gc = get_sheets_client()
        sh = gc.open_by_key(GOOGLE_SHEETS_ID)

        # Get or create tab
        try:
            worksheet = sh.worksheet(tab)
        except gspread.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=tab, rows=1000, cols=20)
            if tab == "Raw Leads":
                worksheet.append_row([
                    "Timestamp", "Lead ID", "Name", "Phone", "Email",
                    "Service Type", "Address", "VIN/Vehicle", "Additional Details"
                ])
            elif tab == "CRM Pipeline":
                worksheet.append_row([
                    "Timestamp", "Name", "Phone", "Email", "Service Type",
                    "Status", "Quote Sent", "Booked Slot", "Follow-Up 1", "Follow-Up 2"
                ])
            elif tab == "Dead Letters":
                worksheet.append_row([
                    "Timestamp", "Name", "Phone", "Email", "Service Type", "Reason"
                ])

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if tab == "Raw Leads":
            vehicle = build_vehicle_string(lead)
            worksheet.append_row([
                timestamp,
                lead.get("lead_id", ""),
                lead.get("name", ""),
                lead.get("phone", ""),
                lead.get("email", ""),
                lead.get("service_type", ""),
                lead.get("address", ""),
                vehicle,
                lead.get("additional_details", "")
            ])
        elif tab == "Dead Letters":
            worksheet.append_row([
                timestamp,
                lead.get("name", ""),
                lead.get("phone", ""),
                lead.get("email", ""),
                lead.get("service_type", ""),
                (extra or {}).get("reason", "Unknown")
            ])

    except Exception as e:
        logger.error(f"Sheets logging failed: {e}")

def update_crm(lead: dict, status: str, booked_slot: str = "",
               quote_sent: bool = False, sms_sent: bool = False):
    """Update or add row in CRM Pipeline tab"""
    try:
        gc = get_sheets_client()
        sh = gc.open_by_key(GOOGLE_SHEETS_ID)

        try:
            ws = sh.worksheet("CRM Pipeline")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="CRM Pipeline", rows=1000, cols=20)
            ws.append_row([
                "Timestamp", "Name", "Phone", "Email", "Service Type",
                "Status", "Quote Sent", "Booked Slot", "Follow-Up 1", "Follow-Up 2"
            ])

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([
            timestamp,
            lead.get("name", ""),
            lead.get("phone", ""),
            lead.get("email", ""),
            lead.get("service_type", ""),
            status,
            "Yes" if quote_sent else "No",
            booked_slot or "",
            "Pending",
            "Pending"
        ])
    except Exception as e:
        logger.error(f"CRM update failed: {e}")

# ─── Follow-Up Engine ─────────────────────────────────────────────────────────
scheduler = BackgroundScheduler()

def check_followups():
    """Check CRM for leads needing follow-up (runs every 30 minutes)"""
    try:
        gc = get_sheets_client()
        sh = gc.open_by_key(GOOGLE_SHEETS_ID)
        try:
            ws = sh.worksheet("CRM Pipeline")
        except gspread.exceptions.WorksheetNotFound:
            # Tab doesn't exist yet — create it and wait for real leads
            ws = sh.add_worksheet(title="CRM Pipeline", rows=1000, cols=20)
            ws.append_row([
                "Timestamp", "Name", "Phone", "Email", "Service Type",
                "Status", "Quote Sent", "Booked Slot", "Follow-Up 1", "Follow-Up 2"
            ])
            logger.info("Created CRM Pipeline tab — no leads to follow up on yet")
            return  # Nothing to follow up on yet
        rows = ws.get_all_records()

        now = datetime.now()
        for i, row in enumerate(rows, start=2):  # start=2 because row 1 is header
            try:
                lead_time = datetime.strptime(row.get("Timestamp", ""), "%Y-%m-%d %H:%M:%S")
                hours_elapsed = (now - lead_time).total_seconds() / 3600
                phone = row.get("Phone", "")
                name = row.get("Name", "").split()[0] if row.get("Name") else "there"
                service = row.get("Service Type", "glass service")

                # 2-hour follow-up
                if 2 <= hours_elapsed < 3 and row.get("Follow-Up 1") == "Pending":
                    msg = (
                        f"Hi {name}! Just following up from AiGlass+ (206) 775-1567. "
                        f"Did you have any questions about your {service} request? "
                        f"We're ready to help today!"
                    )
                    send_sms(phone, msg)
                    ws.update_cell(i, 9, f"Sent {now.strftime('%m/%d %H:%M')}")
                    logger.info(f"Follow-up 1 sent to {phone}")

                # 24-hour follow-up
                elif 24 <= hours_elapsed < 25 and row.get("Follow-Up 2") == "Pending":
                    msg = (
                        f"Hi {name}, AiGlass+ here. We still have availability this week "
                        f"for your {service}. Same-day service available!\n"
                        f"Book your slot in 60 seconds: {BOOKING_URL}\n"
                        f"Or call/text (206) 775-1567."
                    )
                    send_sms(phone, msg)
                    ws.update_cell(i, 10, f"Sent {now.strftime('%m/%d %H:%M')}")
                    logger.info(f"Follow-up 2 sent to {phone}")

            except Exception as row_err:
                logger.warning(f"Follow-up check error on row {i}: {row_err}")

    except Exception as e:
        logger.error(f"Follow-up engine error: {e}")

scheduler.add_job(check_followups, "interval", minutes=30, id="followup_engine")
scheduler.start()

# ─── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    logger.info("AiGlass+ Automation Server started")
    logger.info(f"Calendar: {CALENDAR_ID}")
    logger.info(f"Twilio: {TWILIO_FROM_NUMBER}")
    logger.info(f"Sheets: {GOOGLE_SHEETS_ID}")
    logger.info("Follow-up scheduler running every 30 minutes")

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()
    logger.info("Server shutting down")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
