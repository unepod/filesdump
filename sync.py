#!/usr/bin/env python3
import os
import sys
import pickle
import requests
from datetime import datetime, timedelta, timezone
from icalendar import Calendar
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ============ CONFIGURATION =============
ICS_URL = "https://mail.atom.team/owa/calendar/580df3b4137c4387a06f0f67885fab00@atom.team/355f20453879493ea9a7b7e266d07a1e5320367143650742990/calendar.ics"
GOOGLE_CALENDAR_ID = "690bde798429ed2ed5de692bcd860c0718784ef55d4b62ace34f396fd6784d6a@group.calendar.google.com"
SYNC_MARKER = "exchange-sync"
# ========================================

SCOPES = ["https://www.googleapis.com/auth/calendar"]

def get_google_service():
    creds = None
    token_path = os.path.join(os.path.dirname(__file__), "token.pickle")
    creds_path = os.path.join(os.path.dirname(__file__), "credentials.json")
    
    if os.path.exists(token_path):
        with open(token_path, "rb") as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = Flow.from_client_secrets_file(
                creds_path,
                scopes=SCOPES,
                redirect_uri="urn:ietf:wg:oauth:2.0:oob"
            )
            auth_url, _ = flow.authorization_url(prompt="consent")
            print(f"\nPlease visit this URL to authorize:\n\n{auth_url}\n")
            code = input("Enter the authorization code: ")
            flow.fetch_token(code=code)
            creds = flow.credentials
        with open(token_path, "wb") as token:
            pickle.dump(creds, token)
    
    return build("calendar", "v3", credentials=creds)

def get_timezone_name(dt):
    if dt.tzinfo:
        tz_name = str(dt.tzinfo)
        tz_map = {
            'Europe/Moscow': 'Europe/Moscow',
            'Russian Standard Time': 'Europe/Moscow',
            'China Standard Time': 'Asia/Shanghai',
            'UTC': 'UTC',
        }
        for key, val in tz_map.items():
            if key in tz_name:
                return val
    return 'UTC'

def fetch_ics_events():
    response = requests.get(ICS_URL, timeout=60)
    response.raise_for_status()
    cal = Calendar.from_ical(response.content)
    
    events = []
    for component in cal.walk():
        if component.name == "VEVENT":
            uid = str(component.get("uid", ""))
            summary = str(component.get("summary", "No Title"))
            description = str(component.get("description", "")) if component.get("description") else ""
            location = str(component.get("location", "")) if component.get("location") else ""
            
            dtstart = component.get("dtstart")
            dtend = component.get("dtend")
            
            if not dtstart:
                continue
            
            start = dtstart.dt
            end = dtend.dt if dtend else start
            
            rrule = component.get("rrule")
            rrule_str = None
            if rrule:
                rrule_str = rrule.to_ical().decode("utf-8")
            
            exdate = component.get("exdate")
            exdate_list = []
            if exdate:
                if isinstance(exdate, list):
                    for ex in exdate:
                        for dt in ex.dts:
                            exdate_list.append(dt.dt)
                else:
                    for dt in exdate.dts:
                        exdate_list.append(dt.dt)
            
            recurrence_id = component.get("recurrence-id")
            rec_id_dt = None
            if recurrence_id:
                rec_id_dt = recurrence_id.dt
            
            events.append({
                "uid": uid,
                "summary": summary,
                "description": description,
                "location": location,
                "start": start,
                "end": end,
                "rrule": rrule_str,
                "exdate": exdate_list,
                "recurrence_id": rec_id_dt,
            })
    
    return events

def dt_to_google(dt):
    if hasattr(dt, "hour"):
        tz = get_timezone_name(dt)
        return {"dateTime": dt.isoformat(), "timeZone": tz}
    else:
        return {"date": dt.isoformat()}

def format_exdate(exdt, is_all_day=False):
    if is_all_day or not hasattr(exdt, "hour"):
        return f"EXDATE;VALUE=DATE:{exdt.strftime('%Y%m%d')}"
    else:
        if exdt.tzinfo:
            utc_dt = exdt.astimezone(timezone.utc)
        else:
            utc_dt = exdt
        return f"EXDATE:{utc_dt.strftime('%Y%m%dT%H%M%SZ')}"

def normalize_datetime_for_comparison(dt):
    """Normalize datetime to UTC for comparison, handling both aware and naive datetimes."""
    if not hasattr(dt, 'hour'):
        # It's a date, not datetime
        return dt
    if dt.tzinfo is None:
        # Naive datetime, assume UTC
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def find_instance_event_id(service, recurring_event_id, original_start_dt):
    """
    Find the Google Calendar instance event ID for a specific occurrence of a recurring event.
    Returns the instance's event ID or None if not found.
    """
    try:
        # Normalize the target datetime
        target_dt = normalize_datetime_for_comparison(original_start_dt)
        is_all_day = not hasattr(original_start_dt, 'hour')
        
        # Calculate time range to search (day before to day after the target)
        if is_all_day:
            time_min = datetime.combine(original_start_dt, datetime.min.time()).replace(tzinfo=timezone.utc)
            time_max = datetime.combine(original_start_dt + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone.utc)
        else:
            time_min = (target_dt - timedelta(days=1))
            time_max = (target_dt + timedelta(days=1))
        
        # Get instances of the recurring event in the time range
        instances = service.events().instances(
            calendarId=GOOGLE_CALENDAR_ID,
            eventId=recurring_event_id,
            timeMin=time_min.isoformat(),
            timeMax=time_max.isoformat(),
            maxResults=10
        ).execute()
        
        for instance in instances.get('items', []):
            instance_start = instance.get('originalStartTime', instance.get('start', {}))
            
            if is_all_day:
                # Compare dates
                instance_date = instance_start.get('date')
                if instance_date == original_start_dt.isoformat():
                    return instance['id']
            else:
                # Compare datetimes
                instance_dt_str = instance_start.get('dateTime')
                if instance_dt_str:
                    # Parse the instance datetime
                    instance_dt = datetime.fromisoformat(instance_dt_str.replace('Z', '+00:00'))
                    instance_dt_utc = instance_dt.astimezone(timezone.utc)
                    
                    # Compare with some tolerance (within same minute)
                    if abs((instance_dt_utc - target_dt).total_seconds()) < 120:
                        return instance['id']
        
        return None
        
    except HttpError as e:
        print(f"  Error finding instance: {e}")
        return None

def sync_to_google(service, ics_events):
    # Separate master events from exception instances
    master_events = {}
    exception_events = []
    
    for event in ics_events:
        if event["recurrence_id"]:
            exception_events.append(event)
        else:
            # Both recurring (with rrule) and single events go here
            master_events[event["uid"]] = event
    
    print(f"  Masters/Singles: {len(master_events)}, Exceptions: {len(exception_events)}")
    
    # Get all existing synced events from Google Calendar
    existing_events = {}
    page_token = None
    while True:
        response = service.events().list(
            calendarId=GOOGLE_CALENDAR_ID,
            privateExtendedProperty=f"syncMarker={SYNC_MARKER}",
            maxResults=2500,
            pageToken=page_token
        ).execute()
        
        for event in response.get("items", []):
            uid = event.get("extendedProperties", {}).get("private", {}).get("uid")
            if uid:
                existing_events[uid] = event
        
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    
    # ===== PASS 1: Sync master events (recurring and single) =====
    current_uids = set()
    created = 0
    updated = 0
    errors = 0
    
    # Keep track of Google event IDs for recurring events (needed for exception sync)
    uid_to_google_id = {}
    
    for uid, ics_event in master_events.items():
        current_uids.add(uid)
        
        is_all_day = not hasattr(ics_event["start"], "hour")
        
        google_event = {
            "summary": ics_event["summary"],
            "description": ics_event["description"],
            "location": ics_event["location"],
            "start": dt_to_google(ics_event["start"]),
            "end": dt_to_google(ics_event["end"]),
            "extendedProperties": {
                "private": {
                    "uid": uid,
                    "syncMarker": SYNC_MARKER
                }
            }
        }
        
        if ics_event["rrule"]:
            recurrence = [f"RRULE:{ics_event['rrule']}"]
            for exdt in ics_event["exdate"]:
                recurrence.append(format_exdate(exdt, is_all_day))
            google_event["recurrence"] = recurrence
        
        try:
            if uid in existing_events:
                result = service.events().update(
                    calendarId=GOOGLE_CALENDAR_ID,
                    eventId=existing_events[uid]["id"],
                    body=google_event
                ).execute()
                uid_to_google_id[uid] = result["id"]
                updated += 1
            else:
                result = service.events().insert(
                    calendarId=GOOGLE_CALENDAR_ID,
                    body=google_event
                ).execute()
                uid_to_google_id[uid] = result["id"]
                created += 1
        except HttpError as e:
            print(f"Error syncing event '{ics_event['summary']}': {e}")
            errors += 1
    
    # Also populate uid_to_google_id from existing events
    for uid, event in existing_events.items():
        if uid not in uid_to_google_id:
            uid_to_google_id[uid] = event["id"]
    
    # ===== PASS 2: Sync exception instances =====
    exceptions_updated = 0
    exceptions_errors = 0
    
    for exc_event in exception_events:
        uid = exc_event["uid"]
        rec_id = exc_event["recurrence_id"]
        
        # Find the parent recurring event's Google Calendar ID
        parent_google_id = uid_to_google_id.get(uid)
        
        if not parent_google_id:
            print(f"  Warning: No parent found for exception '{exc_event['summary']}' (UID: {uid[:30]}...)")
            exceptions_errors += 1
            continue
        
        # Find the specific instance in Google Calendar
        instance_id = find_instance_event_id(service, parent_google_id, rec_id)
        
        if not instance_id:
            print(f"  Warning: Could not find instance for '{exc_event['summary']}' at {rec_id}")
            exceptions_errors += 1
            continue
        
        # Update the instance with the modified data
        instance_update = {
            "summary": exc_event["summary"],
            "description": exc_event["description"],
            "location": exc_event["location"],
            "start": dt_to_google(exc_event["start"]),
            "end": dt_to_google(exc_event["end"]),
        }
        
        try:
            service.events().patch(
                calendarId=GOOGLE_CALENDAR_ID,
                eventId=instance_id,
                body=instance_update
            ).execute()
            exceptions_updated += 1
        except HttpError as e:
            print(f"  Error updating exception '{exc_event['summary']}': {e}")
            exceptions_errors += 1
    
    # ===== Delete events no longer in ICS =====
    deleted = 0
    for uid, event in existing_events.items():
        if uid not in current_uids:
            try:
                service.events().delete(
                    calendarId=GOOGLE_CALENDAR_ID,
                    eventId=event["id"]
                ).execute()
                deleted += 1
            except HttpError as e:
                print(f"Error deleting event: {e}")
    
    total_errors = errors + exceptions_errors
    return created, updated, deleted, total_errors, exceptions_updated

def main():
    print(f"[{datetime.now()}] Starting sync...")
    
    try:
        service = get_google_service()
        ics_events = fetch_ics_events()
        print(f"Fetched {len(ics_events)} events from Exchange")
        
        created, updated, deleted, errors, exceptions_updated = sync_to_google(service, ics_events)
        print(f"Sync complete: {created} created, {updated} updated, {deleted} deleted, {errors} errors")
        print(f"  Exception instances updated: {exceptions_updated}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
