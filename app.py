import io, re, datetime as dt
import os
from flask import Flask, redirect, url_for, session, request, render_template, send_from_directory
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pdfplumber
import pandas as pd
import json # To handle credentials dict

# --- Flask App Setup ---
app = Flask(__name__)
# Flask needs a secret key for session management (e.g., storing user credentials)
# IMPORTANT: In production, generate a strong random key and set it as an environment variable.
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "a_very_secret_key_that_you_should_change_in_production")

# --- CONFIG (from environment variables) ---
# Ensure these environment variables are set on your hosting service!
CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = os.environ.get("GOOGLE_REDIRECT_URI") # This MUST be your app's new URL + /oauth2callback

SCOPES = [
    "https://www.googleapis.com/auth/calendar.events",
    "https://www.googleapis.com/auth/calendar",
    "openid",
    "https://www.googleapis.com/auth/userinfo.email"
]

# --- PDF Parsing Configuration ---

# Mapping for month names from PDF to standard numeric representation
# IMPORTANT: These keys MUST match the month names as they appear in your PDF
MONTH_MAP = {
    "GENER": "01", "FEBRER": "02", "MARÇ": "03", "ABRIL": "04",
    "MAIG": "05", "JUNY": "06", "JULIOL": "07", "AGOST": "08",
    "SETEMBRE": "09", "OCTUBRE": "10", "NOVEMBRE": "11", "DESEMBRE": "12"
}

# Define the precise bounding box coordinates for each month's data table
# Format: {page_number: {month_name: (x0, y0, x1, y1)}}
MONTH_DATA_BOUNDING_BOXES = {
    1: { # Page 1
        "GENER": (113, 130, 240, 832),
        "FEBRER": (253, 130, 380, 832),
        "MARÇ": (393, 130, 517, 832)
    },
    2: { # Page 2
        "ABRIL": (21, 130, 146, 832),
        "MAIG": (156, 130, 280, 832),
        "JUNY": (290, 130, 415, 832),
        "JULIOL": (425, 130, 550, 832)
    },
    3: { # Page 3
        "AGOST": (20, 130, 145, 832),
        "SETEMBRE": (155, 130, 280, 832),
        "OCTUBRE": (290, 130, 414, 832),
        "NOVEMBRE": (424, 130, 549, 832)
    },
    4: { # Page 4
        "DESEMBRE": (20, 130, 239, 832)
    }
}

# Define table extraction settings
TABLE_SETTINGS = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
    "snap_tolerance": 5,
    "join_tolerance": 5,
    "edge_min_length": 3,
    # Add other settings as needed for your specific PDF structure
}


# --- Helper Functions ---

def make_flow(state=None):
    """Initializes and returns the Google OAuth Flow object."""
    if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
        raise ValueError("Missing GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, or GOOGLE_REDIRECT_URI environment variables.")

    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [REDIRECT_URI], # Must match the URI Google redirects to
            }
        },
        scopes=SCOPES,
        state=state
    )
    flow.redirect_uri = REDIRECT_URI # Ensure flow object uses the correct redirect URI
    return flow

def creds_from_dict(d):
    """Converts a dictionary back into a Google OAuth Credentials object."""
    if not d:
        return None
    try:
        # Pass the dictionary unpacked to Credentials constructor
        creds = Credentials(**d)
        return creds
    except Exception as e:
        app.logger.error(f"Error restoring credentials from dict: {e}")
        return None

def creds_to_dict(creds):
    """Converts a Google OAuth Credentials object to a dictionary for session storage."""
    return {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
        "id_token": creds.id_token,
    }


def parse_pdf(data, target_months=None):
    """
    Parses a PDF for shift data, optionally filtering by specific months.

    Args:
        data (bytes): The raw PDF file content as bytes.
        target_months (list, optional): A list of month names (e.g., ["GENER", "FEBRER"])
                                        to process. If None, all configured months are processed.

    Returns:
        list: A list of dictionaries, each representing a shift.
    """
    shifts = []
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            app.logger.info(f"Opened PDF for parsing.")

            # Try to get year from metadata title, default to current year
            year_match = re.search(r"(\d{4})", pdf.metadata.get("Title", ""))
            year = int(year_match.group(1)) if year_match else dt.datetime.now().year
            app.logger.info(f"Detected year: {year}")

            for page_num, page_obj in enumerate(pdf.pages, 1):
                app.logger.info(f"Processing Page {page_num}...")

                # Get the months expected on this page from our bounding box config
                months_on_this_page = MONTH_DATA_BOUNDING_BOXES.get(page_num, {})

                if not months_on_this_page:
                    app.logger.info(f"No specific months configured for Page {page_num}. Skipping.")
                    continue

                for month_name, bbox in months_on_this_page.items():
                    # If target_months are specified, skip months not in the target list
                    if target_months and month_name not in target_months:
                        app.logger.info(f"  Skipping {month_name} as it's not in the selected months.")
                        continue

                    app.logger.info(f"  Extracting data for {month_name} using bbox: {bbox}...")
                    
                    # Crop the page to the specific month's bounding box
                    cropped_page = page_obj.crop(bbox)

                    # Extract tables from the cropped region using defined settings
                    tables = cropped_page.extract_tables(TABLE_SETTINGS)

                    if not tables:
                        app.logger.warning(f"    No tables found for {month_name} within specified bbox.")
                        continue

                    # Assuming the first table found in the cropped region is the main data table
                    table_data = tables[0]

                    # --- Start of specific table data processing ---
                    # Find the row that contains the headers for the day numbers and Entrada/Sortida
                    header_row_index = -1
                    for r_idx, row in enumerate(table_data):
                        if row and any(h.strip().lower() in ["dia", "entrada", "sortida"] for h in row if h):
                            header_row_index = r_idx
                            break
                    
                    if header_row_index == -1:
                        app.logger.warning(f"    Could not find header row for {month_name}. Skipping table.")
                        continue
                    
                    # Get headers, skipping empty cells
                    raw_headers = table_data[header_row_index]
                    
                    # Find column indices for 'Entrada' and 'Sortida'
                    entrada_col_idx = -1
                    sortida_col_idx = -1
                    
                    for i, header_cell in enumerate(raw_headers):
                        if header_cell:
                            cleaned_header = header_cell.strip().lower()
                            if "entrada" in cleaned_header:
                                entrada_col_idx = i
                            elif "sortida" in cleaned_header:
                                sortida_col_idx = i
                    
                    if entrada_col_idx == -1 or sortida_col_idx == -1:
                        app.logger.warning(f"    'Entrada' or 'Sortida' columns not found in {month_name}'s table. Skipping.")
                        continue

                    # Iterate through rows starting from the row *after* the header row
                    for row_idx in range(header_row_index + 1, len(table_data)):
                        row = table_data[row_idx]
                        
                        # Assuming the first cell in the data row is the day number
                        day_str = row[0].strip() if row and len(row) > 0 and row[0] else None

                        if not day_str or not day_str.isdigit():
                            continue # Skip non-day rows (e.g., month name, summary rows)

                        try:
                            day = int(day_str)
                            # Get month number from MONTH_MAP using the month_name
                            month_num_str = MONTH_MAP.get(month_name)
                            if not month_num_str:
                                app.logger.warning(f"    Month '{month_name}' not found in MONTH_MAP. Skipping day {day}.")
                                continue
                            
                            current_date = dt.date(year, int(month_num_str), day)
                        except ValueError:
                            app.logger.warning(f"    Invalid day '{day_str}' or month '{month_name}' for date creation. Skipping.")
                            continue

                        # Extract start and end times using their determined column indices
                        start_time_str = row[entrada_col_idx].strip() if len(row) > entrada_col_idx and row[entrada_col_idx] else None
                        end_time_str = row[sortida_col_idx].strip() if len(row) > sortida_col_idx and row[sortida_col_idx] else None

                        # Validate time formats
                        if (start_time_str and re.fullmatch(r"(\d{1,2}):(\d{2})", start_time_str) and
                            end_time_str and re.fullmatch(r"(\d{1,2}):(\d{2})", end_time_str)):
                            
                            try:
                                start_dt_obj = dt.datetime.fromisoformat(f"{current_date.isoformat()}T{start_time_str}")
                                end_dt_obj = dt.datetime.fromisoformat(f"{current_date.isoformat()}T{end_time_str}")

                                # Handle overnight shifts (end time on next day)
                                if end_dt_obj < start_dt_obj:
                                    end_dt_obj += dt.timedelta(days=1)

                                # Create a unique key for tracking shifts
                                key = f"{current_date:%Y%m%d}-{start_time_str.replace(':','')}-{end_time_str.replace(':','')}"
                                shifts.append({
                                    "key": key,
                                    "date": current_date.isoformat(),
                                    "start": start_time_str,
                                    "end": end_time_str
                                })
                            except ValueError as ve:
                                app.logger.warning(f"    Error parsing time for {month_name} {day}: {ve}. Skipping.")
                        else:
                            app.logger.info(f"    No valid shift times found for {month_name} {day}. Skipping.")
                    # --- End of specific table data processing ---

    except Exception as e:
        app.logger.error(f"An error occurred while parsing the PDF: {e}")
        return []

    return shifts

def sync(creds, shifts, tz="Europe/Madrid"):
    inserts, updates, deletes = 0, 0, 0
    try:
        service = build("calendar", "v3", credentials=creds, cache_discovery=False)
        now = dt.datetime.utcnow().isoformat() + "Z" # 'Z' indicates UTC time

        # Fetch existing events that were created by this app
        existing_events = []
        page_token = None
        while True:
            try:
                events_result = service.events().list(
                    calendarId="primary", # Sync to primary calendar
                    timeMin=now,
                    privateExtendedProperty="shiftUploader=1", # Custom property to identify our app's events
                    pageToken=page_token
                ).execute()
                existing_events.extend(events_result.get("items", []))
                page_token = events_result.get('nextPageToken')
                if not page_token: break # No more pages
            except HttpError as error:
                app.logger.error(f"Error fetching existing calendar events: {error.status_code} - {error.reason}")
                return 0, 0, 0

        # Map existing events by their unique key
        by_key = {}
        for e in existing_events:
            if "extendedProperties" in e and "private" in e["extendedProperties"] and "key" in e["extendedProperties"]["private"]:
                by_key[e["extendedProperties"]["private"]["key"]] = e

        for s in shifts:
            start_dt_obj = dt.datetime.fromisoformat(f"{s['date']}T{s['start']}")
            end_dt_obj = dt.datetime.fromisoformat(f"{s['date']}T{s['end']}")
            if end_dt_obj < start_dt_obj: # Adjust end date for overnight shifts
                end_dt_obj += dt.timedelta(days=1)

            start_iso = start_dt_obj.isoformat(timespec='seconds')
            end_iso = end_dt_obj.isoformat(timespec='seconds')

            body = {
                "summary": f"P {s['start']}-{s['end']}", # Event title
                "start": {"dateTime": start_iso, "timeZone": tz},
                "end":   {"dateTime": end_iso,   "timeZone": tz},
                "extendedProperties": {"private": {"shiftUploader": "1", "key": s["key"]}},
            }

            try:
                if s["key"] in by_key: # Update existing event
                    ev_id = by_key[s["key"]]["id"]
                    service.events().patch(calendarId="primary", eventId=ev_id, body=body).execute()
                    updates += 1
                    del by_key[s["key"]] # Mark as processed
                else: # Insert new event
                    service.events().insert(calendarId="primary", body=body).execute()
                    inserts += 1
            except HttpError as error:
                app.logger.error(f"Error syncing event '{s['key']}': {error.status_code} - {error.reason}")

        # Delete remaining events in by_key (they are no longer in the PDF)
        for ev in by_key.values():
            try:
                service.events().delete(calendarId="primary", eventId=ev["id"]).execute()
            except HttpError as error:
                app.logger.error(f"Error deleting event with ID '{ev['id']}': {error.status_code} - {error.reason}")

        return inserts, updates, deletes

    except Exception as e:
        app.logger.error(f"An unexpected error occurred during calendar sync: {e}")
        return 0, 0, 0

# --- NEW FUNCTION TO DELETE ALL APP-CREATED EVENTS ---
def delete_all_app_events(creds, tz="Europe/Madrid"):
    deleted_count = 0
    try:
        service = build("calendar", "v3", credentials=creds, cache_discovery=False)
        
        # Fetch all events created by this app (using the private extended property)
        # Note: We're not using timeMin=now here, to delete past events too
        app_events = []
        page_token = None
        while True:
            try:
                events_result = service.events().list(
                    calendarId="primary",
                    privateExtendedProperty="shiftUploader=1", # Only fetch events with this flag
                    pageToken=page_token
                ).execute()
                app_events.extend(events_result.get("items", []))
                page_token = events_result.get('nextPageToken')
                if not page_token: break
            except HttpError as error:
                app.logger.error(f"Error fetching events for deletion: {error.status_code} - {error.reason}")
                return 0

        if not app_events:
            app.logger.info("No app-created events found to delete.")
            return 0

        # Delete each identified event
        for event in app_events:
            try:
                service.events().delete(calendarId="primary", eventId=event["id"]).execute()
                deleted_count += 1
                app.logger.info(f"Deleted event: {event.get('summary', 'No Summary')} (ID: {event['id']})")
            except HttpError as error:
                app.logger.error(f"Error deleting event ID '{event['id']}': {error.status_code} - {error.reason}")
                # Continue trying to delete other events even if one fails

        return deleted_count

    except Exception as e:
        app.logger.error(f"An unexpected error occurred during bulk deletion: {e}")
        return 0


# --- Flask Routes ---

@app.route("/")
def index():
    creds_data = session.get("creds")
    creds = creds_from_dict(creds_data)

    # Check for messages/errors passed from redirects
    message = request.args.get("message")
    error = request.args.get("error")

    # Get all available months from the bounding box configuration for the HTML dropdown
    # We create a dictionary for display purposes, mapping internal month names to Catalan display names
    available_months_display = {
        "GENER": "Gener", "FEBRER": "Febrer", "MARÇ": "Març", "ABRIL": "Abril",
        "MAIG": "Maig", "JUNY": "Juny", "JULIOL": "Juliol", "AGOST": "Agost",
        "SETEMBRE": "Setembre", "OCTUBRE": "Octubre", "NOVEMBRE": "Novembre", "DESEMBRE": "Desembre"
    }
    
    # Create a list of (internal_name, display_name) tuples for sorting and template use
    available_months_for_template = sorted([
        (month_key, available_months_display.get(month_key, month_key))
        for page_months in MONTH_DATA_BOUNDING_BOXES.values()
        for month_key in page_months.keys()
    ], key=lambda x: list(available_months_display.keys()).index(x[0]) if x[0] in available_months_display else x[0])


    if creds and creds.valid:
        return render_template("index.html", logged_in=True, message=message, error=error, available_months=available_months_for_template)
    elif creds and creds.expired and creds.refresh_token:
        # Attempt to refresh token
        try:
            flow = make_flow()
            flow.credentials = creds
            flow.refresh_credentials()
            session["creds"] = creds_to_dict(flow.credentials)
            return render_template("index.html", logged_in=True, message="Token refrescat amb èxit!", error=error, available_months=available_months_for_template)
        except Exception as e:
            app.logger.error(f"Failed to refresh token: {e}")
            session.pop("creds", None) # Clear invalid credentials
            return render_template("index.html", logged_in=False, error="No s'ha pogut actualitzar el testimoni. Torna a iniciar sessió.")
    else:
        # Not logged in or invalid/unrefreshable creds
        session.pop("creds", None) # Ensure old/bad creds are cleared
        return render_template("index.html", logged_in=False, message=message, error=error)

@app.route("/google_login")
def google_login():
    try:
        flow = make_flow()
        authorization_url, state = flow.authorization_url(
            access_type="offline", # Get a refresh token
            prompt="consent",      # Ensure consent screen is shown
            include_granted_scopes="true" # Include previously granted scopes
        )
        session["oauth_state"] = state # Store state for validation later
        return redirect(authorization_url)
    except ValueError as e:
        app.logger.error(f"Configuration error for Google login: {e}")
        return render_template("index.html", logged_in=False, error=f"Error de configuració de l'aplicació: {e}. Comprova les variables d'entorn.")
    except Exception as e:
        app.logger.error(f"Error during Google login initiation: {e}")
        return render_template("index.html", logged_in=False, error="No s'ha pogut iniciar la sessió amb Google.")


@app.route("/oauth2callback")
def oauth2callback():
    code = request.args.get("code")
    state = request.args.get("state")
    error = request.args.get("error")

    if error:
        app.logger.error(f"OAuth callback error: {error}")
        return render_template("index.html", logged_in=False, error=f"Sessió amb Google denegada o error: {error}")

    if not code:
        app.logger.error("OAuth callback received no code.")
        return render_template("index.html", logged_in=False, error="Autenticació fallida: No s'ha rebut cap codi.")

    if state != session.get("oauth_state"):
        app.logger.error("OAuth state mismatch.")
        return render_template("index.html", logged_in=False, error="Autenticació fallida: Desajustament d'estat.")

    try:
        flow = make_flow(state=state)
        # Exchange the authorization code for tokens
        flow.fetch_token(code=code)

        session["creds"] = creds_to_dict(flow.credentials)
        session.pop("oauth_state", None) # Clear state after successful use

        return redirect(url_for("index", message="Sessió iniciada amb Google amb èxit!"))
    except Exception as e:
        app.logger.error(f"Error during token exchange: {e}")
        session.pop("creds", None) # Clear invalid credentials
        session.pop("oauth_state", None)
        return render_template("index.html", logged_in=False, error=f"Autenticació fallida: {e}. Torna-ho a provar.")

@app.route("/upload_pdf", methods=["POST"])
def upload_pdf():
    creds_data = session.get("creds")
    creds = creds_from_dict(creds_data)

    # Get all available months from the bounding box configuration for the HTML dropdown
    available_months_display = {
        "GENER": "Gener", "FEBRER": "Febrer", "MARÇ": "Març", "ABRIL": "Abril",
        "MAIG": "Maig", "JUNY": "Juny", "JULIOL": "Juliol", "AGOST": "Agost",
        "SETEMBRE": "Setembre", "OCTUBRE": "Octubre", "NOVEMBRE": "Novembre", "DESEMBRE": "Desembre"
    }
    available_months_for_template = sorted([
        (month_key, available_months_display.get(month_key, month_key))
        for page_months in MONTH_DATA_BOUNDING_BOXES.values()
        for month_key in page_months.keys()
    ], key=lambda x: list(available_months_display.keys()).index(x[0]) if x[0] in available_months_display else x[0])


    if not creds or not creds.valid:
        return redirect(url_for("index", error="Has d'iniciar sessió per pujar un PDF."))

    if 'pdf_file' not in request.files:
        return render_template("index.html", logged_in=True, error="No s'ha trobat cap fitxer a la sol·licitud.", available_months=available_months_for_template)

    pdf_file = request.files['pdf_file']
    if pdf_file.filename == '':
        return render_template("index.html", logged_in=True, error="No s'ha seleccionat cap fitxer.", available_months=available_months_for_template)

    if pdf_file and pdf_file.filename.lower().endswith('.pdf'):
        pdf_data = pdf_file.read()
        
        # Get selected months from the form.
        selected_months = request.form.getlist('months')
        if not selected_months: # If nothing is selected, process all months by default
            app.logger.info("No specific months selected, parsing all configured months.")
            target_months = None # parse_pdf will process all if None
        else:
            app.logger.info(f"Months selected for parsing: {selected_months}")
            target_months = selected_months

        shifts = parse_pdf(pdf_data, target_months=target_months)

        if not shifts:
            return render_template("index.html", logged_in=True, error="No s'han trobat torns al PDF. Comprova el format (columnes Entrada/Sortida) o els mesos seleccionats.", available_months=available_months_for_template)
        else:
            ins, upd, dele = sync(creds, shifts)
            return render_template("index.html", logged_in=True, message=f"Sincronització completada: {ins} torns inserits, {upd} actualitzats, {dele} eliminats.", available_months=available_months_for_template)
    else:
        return render_template("index.html", logged_in=True, error="Tipus de fitxer invàlid. Puja un PDF.", available_months=available_months_for_template)

@app.route("/delete_all_shifts", methods=["POST"]) # Use POST to avoid accidental deletion
def delete_all_shifts():
    creds_data = session.get("creds")
    creds = creds_from_dict(creds_data)

    # Get all available months from the bounding box configuration for the HTML dropdown
    available_months_display = {
        "GENER": "Gener", "FEBRER": "Febrer", "MARÇ": "Març", "ABRIL": "Abril",
        "MAIG": "Maig", "JUNY": "Juny", "JULIOL": "Juliol", "AGOST": "Agost",
        "SETEMBRE": "Setembre", "OCTUBRE": "Octubre", "NOVEMBRE": "Novembre", "DESEMBRE": "Desembre"
    }
    available_months_for_template = sorted([
        (month_key, available_months_display.get(month_key, month_key))
        for page_months in MONTH_DATA_BOUNDING_BOXES.values()
        for month_key in page_months.keys()
    ], key=lambda x: list(available_months_display.keys()).index(x[0]) if x[0] in available_months_display else x[0])


    if not creds or not creds.valid:
        return redirect(url_for("index", error="Has d'iniciar sessió per eliminar torns."))

    try:
        deleted_count = delete_all_app_events(creds)
        if deleted_count > 0:
            return redirect(url_for("index", message=f"S'han eliminat amb èxit {deleted_count} torns creats per l'aplicació!"))
        else:
            return redirect(url_for("index", message="No s'han trobat torns creats per l'aplicació per eliminar."))
    except Exception as e:
        app.logger.error(f"Failed to delete shifts: {e}")
        return redirect(url_for("index", error=f"No s'han pogut eliminar els torns: {e}"))


@app.route("/logout")
def logout():
    session.pop("creds", None)
    session.pop("oauth_state", None)
    return redirect(url_for("index", message="Sessió tancada amb èxit!"))

# --- Run the app ---
if __name__ == "__main__":
    # In production, Render sets the PORT environment variable.
    # debug=True should be False in production for security.
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
