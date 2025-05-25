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

# --- Helper Functions (Reused from your Streamlit app) ---

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


def parse_pdf(data):
    shifts = []
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            # Try to get year from metadata title, default to current year
            year_match = re.search(r"(\d{4})", pdf.metadata.get("Title", ""))
            year = int(year_match.group(1)) if year_match else dt.datetime.now().year

            for page_num, page in enumerate(pdf.pages, 1):
                table = page.extract_table()
                if not table: continue # Skip if no table found on page
                
                # Basic check for a valid table (at least 2 rows, first row has columns)
                if len(table) < 2 or len(table[0]) == 0: continue
                df = pd.DataFrame(table[1:], columns=table[0])

                # Check if "Entrada" and "Sortida" columns exist
                if "Entrada" not in df.columns or "Sortida" not in df.columns:
                    app.logger.warning(f"Page {page_num}: 'Entrada' or 'Sortida' columns not found. Skipping.")
                    continue

                for col_name in df.columns[1:]: # Iterate through day columns
                    day_str = str(df.iloc[0][col_name]).strip()
                    if not day_str.isdigit(): continue # Skip if not a valid day number (e.g., month name)
                    
                    try:
                        date = dt.date(year, page_num, int(day_str)) # Assuming page_num is month
                    except ValueError:
                        app.logger.warning(f"Invalid date on page {page_num}, column {col_name}: {day_str}. Skipping.")
                        continue

                    # Extract start and end times
                    start_val = df.loc[df["Entrada"] == "Entrada", col_name].values
                    end_val   = df.loc[df["Sortida"] == "Sortida", col_name].values

                    if start_val.size and end_val.size and \
                       re.fullmatch(r"(\d{1,2}):(\d{2})", start_val[0]) and \
                       re.fullmatch(r"(\d{1,2}):(\d{2})", end_val[0]):
                        
                        start_time = str(start_val[0]).strip()
                        end_time = str(end_val[0]).strip()

                        start_dt_obj = dt.datetime.fromisoformat(f"{date.isoformat()}T{start_time}")
                        end_dt_obj = dt.datetime.fromisoformat(f"{date.isoformat()}T{end_time}")
                        
                        # Handle overnight shifts (end time on next day)
                        if end_dt_obj < start_dt_obj:
                            end_dt_obj += dt.timedelta(days=1)
                        
                        # Create a unique key for tracking shifts
                        key = f"{date:%Y%m%d}-{start_time.replace(':','')}-{end_time.replace(':','')}"
                        shifts.append({"key": key, "date": date.isoformat(), "start": start_time, "end": end_time})
        return shifts
    except Exception as e:
        app.logger.error(f"An error occurred while parsing the PDF: {e}")
        # traceback.print_exc() # For more detailed server logs
        return []

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
            end_iso   = end_dt_obj.isoformat(timespec='seconds')

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
                deletes += 1
            except HttpError as error:
                app.logger.error(f"Error deleting event with ID '{ev['id']}': {error.status_code} - {error.reason}")

        return inserts, updates, deletes

    except Exception as e:
        app.logger.error(f"An unexpected error occurred during calendar sync: {e}")
        return 0, 0, 0


# --- Flask Routes ---

@app.route("/")
def index():
    creds_data = session.get("creds")
    creds = creds_from_dict(creds_data)

    if creds and creds.valid:
        return render_template("index.html", logged_in=True)
    elif creds and creds.expired and creds.refresh_token:
        # Attempt to refresh token
        try:
            flow = make_flow()
            flow.credentials = creds
            flow.refresh_credentials()
            session["creds"] = creds_to_dict(flow.credentials)
            return render_template("index.html", logged_in=True, message="Token refreshed successfully!")
        except Exception as e:
            app.logger.error(f"Failed to refresh token: {e}")
            session.pop("creds", None) # Clear invalid credentials
            return render_template("index.html", logged_in=False, error="Failed to refresh token. Please sign in again.")
    else:
        # Not logged in or invalid/unrefreshable creds
        session.pop("creds", None) # Ensure old/bad creds are cleared
        return render_template("index.html", logged_in=False)

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
        return render_template("index.html", logged_in=False, error=f"App configuration error: {e}. Check environment variables.")
    except Exception as e:
        app.logger.error(f"Error during Google login initiation: {e}")
        return render_template("index.html", logged_in=False, error="Could not initiate Google login.")


@app.route("/oauth2callback")
def oauth2callback():
    code = request.args.get("code")
    state = request.args.get("state")
    error = request.args.get("error")

    if error:
        app.logger.error(f"OAuth callback error: {error}")
        return render_template("index.html", logged_in=False, error=f"Google sign-in denied or error: {error}")

    if not code:
        app.logger.error("OAuth callback received no code.")
        return render_template("index.html", logged_in=False, error="Authentication failed: No code received.")

    if state != session.get("oauth_state"):
        app.logger.error("OAuth state mismatch.")
        return render_template("index.html", logged_in=False, error="Authentication failed: State mismatch.")

    try:
        flow = make_flow(state=state)
        # Exchange the authorization code for tokens
        flow.fetch_token(code=code)

        session["creds"] = creds_to_dict(flow.credentials)
        session.pop("oauth_state", None) # Clear state after successful use

        return redirect(url_for("index", message="Successfully signed in with Google!"))
    except Exception as e:
        app.logger.error(f"Error during token exchange: {e}")
        session.pop("creds", None) # Clear invalid credentials
        session.pop("oauth_state", None)
        return render_template("index.html", logged_in=False, error=f"Authentication failed: {e}. Please try again.")

@app.route("/upload_pdf", methods=["POST"])
def upload_pdf():
    creds_data = session.get("creds")
    creds = creds_from_dict(creds_data)

    if not creds or not creds.valid:
        return redirect(url_for("index", error="You need to be signed in to upload a PDF."))

    if 'pdf_file' not in request.files:
        return render_template("index.html", logged_in=True, error="No file part in the request.")

    pdf_file = request.files['pdf_file']
    if pdf_file.filename == '':
        return render_template("index.html", logged_in=True, error="No selected file.")

    if pdf_file and pdf_file.filename.lower().endswith('.pdf'):
        pdf_data = pdf_file.read()
        shifts = parse_pdf(pdf_data)

        if not shifts:
            return render_template("index.html", logged_in=True, error="No shifts found in the PDF. Check format (Entrada/Sortida columns).")
        else:
            # You can store shifts in session if you want to display preview before sync
            # or just proceed directly to sync if that's the desired flow.
            # For simplicity, we'll sync immediately here.
            ins, upd, dele = sync(creds, shifts)
            return render_template("index.html", logged_in=True, message=f"Sync Complete: Inserted {ins}, Updated {upd}, Deleted {dele} shifts.")
    else:
        return render_template("index.html", logged_in=True, error="Invalid file type. Please upload a PDF.")

@app.route("/logout")
def logout():
    session.pop("creds", None)
    session.pop("oauth_state", None)
    return redirect(url_for("index", message="Logged out successfully!"))

# --- Run the app ---
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
