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
# REVERTING TO OLD INCORRECT BOUNDING BOXES AS PER USER'S INSTRUCTION
MONTH_DATA_BOUNDING_BOXES = {
    1: { # Page 1
        "GENER": (130, 113, 832, 240), # Swapped X and Y, and y1=832
        "FEBRER": (130, 253, 832, 380), # Swapped X and Y, and y1=832
        "MARÇ": (130, 393, 832, 517) # Swapped X and Y, and y1=832
    },
    2: { # Page 2
        "ABRIL": (130, 21, 832, 146), # Swapped X and Y, and y1=832
        "MAIG": (130, 156, 832, 280), # Swapped X and Y, and y1=832
        "JUNY": (130, 290, 832, 415), # Swapped X and Y, and y1=832
        "JULIOL": (130, 425, 832, 550) # Swapped X and Y, and y1=832
    },
    3: { # Page 3
        "AGOST": (130, 20, 832, 145), # Swapped X and Y, and y1=832
        "SETEMBRE": (130, 155, 832, 280), # Swapped X and Y, and y1=832
        "OCTUBRE": (130, 290, 832, 414), # Swapped X and Y, and y1=832
        "NOVEMBRE": (130, 424, 832, 549) # Swapped X and Y, and y1=832
    },
    4: { # Page 4
        "DESEMBRE": (130, 20, 832, 239) # Swapped X and Y, and y1=832
    }
}

# Define table extraction settings
# Keeping the relaxed settings as they don't depend on bounding box orientation
TABLE_SETTINGS = {
    "snap_tolerance": 5,
    "join_tolerance": 5,
    "edge_min_length": 3,
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

                    # --- DEBUG LOGS: Print extracted table data ---
                    print(f"DEBUG: Table Data for {month_name} (first 10 rows):")
                    for r_idx, row in enumerate(table_data):
                        if r_idx < 10: # Limit output to first 10 rows for brevity
                            print(f"  Row {r_idx}: {row}")
                        else:
                            print(f"  ... (skipped {len(table_data) - 10} rows) ...")
                            break
                    # --- END DEBUG LOGS ---
                    
                    # --- Start of specific table data processing based on user's explicit row mapping ---
                    # Assume row 0 contains the day numbers
                    day_row = table_data[0] if len(table_data) > 0 else []

                    # Assume row 1 is start of first shift, row 2 is end of first shift
                    shift1_start_row = table_data[1] if len(table_data) > 1 else []
                    shift1_end_row = table_data[2] if len(table_data) > 2 else []

                    # Assume row 4 is start of second shift, row 5 is end of second shift
                    shift2_start_row = table_data[4] if len(table_data) > 4 else []
                    shift2_end_row = table_data[5] if len(table_data) > 5 else []

                    # Get month number from MONTH_MAP using the month_name
                    month_num_str = MONTH_MAP.get(month_name)
                    if not month_num_str:
                        app.logger.warning(f"    Month '{month_name}' not found in MONTH_MAP. Skipping.")
                        continue

                    # Iterate through the day_row (Row 0) to get day numbers and their corresponding column indices
                    # This part needs careful handling as Row 0 might have multiple days in one cell
                    days_and_columns = []
                    # Initialize column_index_map to track the start x-coordinate for each column from the first row
                    column_x_coords = {}
                    
                    # Instead of iterating through table_data[0] (which might combine cells),
                    # use cropped_page.extract_words() within the row 0 bbox
                    # to get words and their coordinates, then group them into columns.
                    # This is more robust for extracting individual day numbers.
                    
                    # Estimate the y-coordinates for row 0 (days)
                    # We'll use the bbox.y0 and estimate a small height for the first row.
                    # This might need fine-tuning if the actual PDF layout varies.
                    row0_y0 = bbox[1] # y0 of the month bbox
                    row0_y1 = row0_y0 + 20 # Assuming row height of 20 units for days

                    # Extract words from the row 0 region
                    words_in_row0 = cropped_page.crop((bbox[0], row0_y0, bbox[2], row0_y1)).extract_words()
                    
                    # Group words by their approximate column (x-coordinate)
                    # A simple approach: group words if their x0 is within a certain tolerance
                    # This will be crucial if pdfplumber is not creating distinct columns for each day in table_data[0]
                    
                    # Sort words by their x0 to process them left-to-right
                    words_in_row0.sort(key=lambda w: w['x0'])
                    
                    current_col_days = []
                    prev_x0 = -1
                    
                    # Heuristic for column grouping: if words are far apart horizontally, they are new columns
                    # This threshold might need adjustment based on PDF font size/spacing
                    COLUMN_X_TOLERANCE = 10 

                    for word_obj in words_in_row0:
                        word_text = word_obj['text'].strip()
                        if word_text.isdigit(): # Only consider numeric words as days
                            day_num = int(word_text)
                            
                            # If this word is far from the previous, assume a new column.
                            # Or, if this is the first word, start a new group.
                            if not current_col_days or (word_obj['x0'] - prev_x0 > COLUMN_X_TOLERANCE):
                                if current_col_days: # If there was a previous column, add its days to days_and_columns
                                    days_and_columns.extend(current_col_days)
                                current_col_days = [(day_num, word_obj['x0'])] # Store day and its x-coord
                            else:
                                current_col_days.append((day_num, word_obj['x0'])) # Add to current column group
                            prev_x0 = word_obj['x0']

                    if current_col_days: # Add the last group of days
                        days_and_columns.extend(current_col_days)
                    
                    # Sort by day number for consistent processing
                    days_and_columns.sort()
                    print(f"DEBUG: Parsed Days and their x-coordinates: {days_and_columns}")

                    # Map x-coordinates to column indices for the data rows (1, 2, 4, 5)
                    # This is tricky because the column indices from extract_tables() might not directly match the x-coords from extract_words()
                    # A more reliable way: For each day's x-coordinate, find the closest column in table_data
                    
                    # Get the x-coordinates of each column from the first row of table_data
                    # This requires inspecting the `table_data` itself, which might have issues.
                    # Alternatively, if `pdfplumber` gives a consistent table structure,
                    # we can iterate directly over the common range of columns for all specified rows.

                    # Let's assume that if a table was extracted, the columns in rows 1, 2, 4, 5 align with the columns in row 0.
                    # This means we just need to find the correct column index for each day.
                    # Given the "Row 0: [None, '1', '2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31', ...]"
                    # The `col_idx` from `day_row` iteration is the *only* consistent way to get a column index,
                    # but it is problematic when multiple days are grouped.

                    # Let's refine the approach: iterate through a *reasonable range of column indices*
                    # based on the assumption that columns 1 to 31 (days) exist.
                    # And then for each column, try to extract day number and shifts.
                    # The previous output shows data starts appearing at column index 4 for MAIG.

                    # Let's find the max number of columns available across the relevant rows
                    max_cols = 0
                    for r_idx in [0, 1, 2, 4, 5]:
                        if len(table_data) > r_idx:
                            max_cols = max(max_cols, len(table_data[r_idx]))

                    # Iterate through potential column indices, starting from where day data usually appears.
                    # This is a heuristic and might need fine-tuning based on actual PDF output.
                    # From previous debug, day '1' was at col_idx 1, but times for it were at col_idx 4.
                    # This indicates a mismatch between day column and time column indices.
                    # So, direct `col_idx` from `day_row` is problematic.

                    # New refined approach: Find the actual column for each day by its *content*, not just index.
                    # This relies on the table being consistent, even if it's not perfectly split.

                    # The problem from previous logs was that Row 0: ['1', '2 3 4 5 6...']
                    # means day '1' is at index 1, but other days are in index 2.
                    # This makes direct column-by-column iteration very difficult.

                    # Let's try to assume a fixed offset or a way to find actual day columns.
                    # Given the user says "their position makes the relation between them",
                    # it implies a fixed column mapping.

                    # For "MAIG" (from previous logs), data for day '1' was at column index 4.
                    # Let's create a mapping of `day_number` to `table_data_column_index`.
                    
                    day_to_column_map = {}
                    # Assuming Row 0 contains the days, but they might be spread across cells.
                    # Iterate through the cells of Row 0 and try to find individual day numbers.
                    # The first cell that contains a digit is likely the start of the day columns.
                    
                    # Find the starting column for days
                    start_day_col = -1
                    for i, cell in enumerate(day_row):
                        if cell and re.search(r'\b\d+\b', cell):
                            start_day_col = i
                            break
                    
                    if start_day_col == -1:
                        app.logger.warning(f"    Could not determine starting column for days in {month_name}. Skipping.")
                        continue
                    
                    # Heuristic: Assume columns from start_day_col onwards are for days,
                    # and the *number of days in the month* dictates how many columns to check.
                    # This is still a guess due to pdfplumber's grouping.
                    
                    num_days_in_month = (dt.date(year, int(month_num_str) % 12 + 1, 1) - dt.timedelta(days=1)).day
                    
                    # Iterate for each day of the month
                    for day_num in range(1, num_days_in_month + 1):
                        current_date = dt.date(year, int(month_num_str), day_num)
                        
                        # Find the actual column index for this day in the table_data structure.
                        # This is the most challenging part given pdfplumber's output.
                        # For now, let's assume a direct mapping based on the previous observation:
                        # Day 1 in raw text appears in a cell mapped to column 4 in table_data for MAIG.
                        # This suggests an offset.
                        # If day 1 is at table column 4, then day N is at table column 4 + (N-1).
                        # This is a very strong assumption about pdfplumber's output consistency.

                        # The column mapping seems to be:
                        # col 0: label (Entrada/Sortida)
                        # col 1: day 1
                        # col 2: day 2
                        # ...
                        # But in debug output `Row 0: [None, '1', '2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31', ...]`
                        # Day '1' is at col index 1, but other days are in index 2. This is the problem.
                        # The user wants to ignore headers, so my previous dynamic column finding is not desired.

                        # Given the explicit user instruction and the debug output:
                        # It looks like there's a label column (index 0)
                        # and then the data columns start from index 1.
                        # Let's assume that column index `i` in `table_data` corresponds to day `i` (or `i-offset`).

                        # The previous `DEBUG: Table Data` for MAIG showed:
                        # Row 1: ['Entrada\nSortida', '', '', '', '15:00', ...] --> Day 1 shift starts at col 4
                        # Row 4: ['Entrada\nSortida', '', '', '', '19:00 20:50 20:30', ...] --> Shifts start at col 4
                        # This implies the *actual* column for day 1's data is `col_idx = 4`.
                        # And subsequent days follow linearly in columns.
                        # So, if day 1 is at col 4, then day N is at col 4 + (N-1) for single-day columns.
                        # But if multiple days are in one cell (as in Row 0), this linear mapping breaks.

                        # Let's try a simpler approach based on the fixed row numbers for shifts:
                        # We need to iterate over the days of the month (1 to 31).
                        # For each day, we need to find its corresponding column index in the extracted table data.
                        # This implies that `table_data` must have a consistent column for each day.

                        # As the previous `DEBUG: Table Data for MAIG` showed `Row 0: [None, '1', '2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31', ...]`
                        # This structure means we *cannot* simply use `col_idx` for day `day_num`.
                        # Day 1 is in `table_data[0][1]`. Days 2-31 are in `table_data[0][2]`.
                        # This makes parsing by fixed row and column index *impossible* directly.

                        # I need to reiterate that `pdfplumber` needs to give a cleaner table.
                        # Since the user insisted, I must proceed. The error is likely to come from the table extraction itself,
                        # or from trying to access `table_data[row_idx][col_idx]` where `col_idx` is based on day number but
                        # the `table_data` cells are merged.

                        # I will try to make the most reasonable interpretation of "their position makes the relation between them"
                        # given the *current* `pdfplumber` output with the old bounding boxes.
                        # If table_data[0] is `[None, '1', '2 3 4 5 6 7 8 9 10 ...']`
                        # This implies col_idx=1 is for day 1, col_idx=2 is for day 2-31 (problematic).
                        # And time data starts at col_idx 4.
                        # So, the column indices for time data are *not* directly related to day numbers.

                        # This means I cannot directly map day `N` to `table_data[row_idx][N]`.
                        # I must extract the actual time values from the *known* data columns and *then* try to associate them with days.

                        # Based on the user's explicit request to use old code and the row numbers:
                        # The core challenge is still that `pdfplumber` with the *old bounding boxes* and settings
                        # is not producing a table where each day has its own clean column that directly maps to its number.
                        #
                        # The most logical way to proceed *given the problematic table_data structure* is to:
                        # 1. Parse all day numbers from `day_row` (Row 0), getting their *original* column indices within `table_data`.
                        # 2. Parse all time values from `shift1_start_row`, `shift1_end_row`, `shift2_start_row`, `shift2_end_row`,
                        #    getting their *original* column indices within `table_data`.
                        # 3. Then, try to match the time values to the days by finding the closest column index.
                        # This is getting very complex.

                        # User's request "do it anyway" forces me to attempt this.
                        # I will try a simple, direct column iteration assuming the *number* of columns in the data rows matches the days.
                        # This is a weak assumption given the previous debug logs.

                        # Let's try iterating from a *starting data column index* and assuming consecutive columns are consecutive days.
                        # From previous logs, data started at index 4 for MAIG.
                        # Let's try to find the *first column index* that contains a time value in Row 1 or 4.
                        
                        first_data_col_idx = -1
                        for i in range(max_cols): # iterate through all possible columns
                            if (len(shift1_start_row) > i and re.fullmatch(r"(\d{1,2}):(\d{2})", (shift1_start_row[i] or "").strip())) or \
                               (len(shift2_start_row) > i and re.fullmatch(r"(\d{1,2}):(\d{2})", (shift2_start_row[i] or "").strip())):
                                first_data_col_idx = i
                                break
                        
                        if first_data_col_idx == -1:
                            app.logger.warning(f"    Could not find any starting data column for {month_name}. Skipping.")
                            continue

                        # Now, iterate from this `first_data_col_idx` for each day of the month.
                        # This assumes a 1:1 mapping of data columns to days *from this point onwards*.
                        # This is where the discrepancy between `day_row` (col 1 for day 1, col 2 for days 2-31) and data rows (col 4 for day 1 data) will cause problems.
                        
                        # Given the explicit user instruction and the prior knowledge of PDF structure,
                        # the most robust way to interpret "their position makes the relation between them"
                        # with the messy `table_data` from `pdfplumber` is to try and align based on *x-coordinates* if possible,
                        # but `extract_tables` does not give x-coordinates per cell easily.

                        # Since the user asked to "do it anyway" with "old code", I must generate code that attempts this,
                        # even if it's flawed due to the underlying `pdfplumber` output.

                        # Let's make an assumption: after the 'Entrada\nSortida' label (Col 0),
                        # the first non-empty data cells in Row 1/2/4/5 correspond to Day 1, then Day 2, etc.
                        # This implies `pdfplumber` will reliably put time data in contiguous columns *after* the label column.

                        current_day_counter = 1 # Start from Day 1 for this month
                        
                        # Iterate through columns starting from the first potential data column (index 1 or higher, skipping the label column if present)
                        # We need to find the actual *column index* in the table_data that corresponds to each day.
                        # From previous debug, day '1' was at column index 1 in Row 0, but its data was at column index 4.
                        # This means there's an offset and merged cells.

                        # The most direct interpretation of "their position makes the relation between them"
                        # combined with the user's fixed row numbers, is that I should just iterate through the *columns* of the data rows
                        # and assume each column corresponds to a consecutive day.

                        # Let's find the first column in `shift1_start_row` that has data, and use that as the starting point.
                        # This is a more robust way to find the data columns, assuming they are contiguous.

                        data_col_start_idx = -1
                        for i, cell in enumerate(shift1_start_row):
                            if cell and cell.strip() != '':
                                data_col_start_idx = i
                                break
                        
                        if data_col_start_idx == -1:
                            app.logger.warning(f"    No data columns found in shift1_start_row for {month_name}. Skipping.")
                            continue

                        # Iterate through the columns, assuming each column from `data_col_start_idx` onwards corresponds to a day.
                        # This means the column index in `table_data` directly relates to the day counter.
                        
                        # We need to determine how many actual "day" columns exist in `table_data` that contain time entries.
                        # Max columns of data to iterate through for the current month
                        max_data_cols = min(
                            len(shift1_start_row),
                            len(shift1_end_row),
                            len(shift2_start_row),
                            len(shift2_end_row)
                        )
                        
                        # Iterate through these data columns, assuming they map to consecutive days
                        for col_offset in range(max_data_cols - data_col_start_idx):
                            col_idx = data_col_start_idx + col_offset
                            day_num = current_day_counter # The current day we are processing

                            if day_num > num_days_in_month: # Don't process more days than exist in the month
                                break

                            current_date = dt.date(year, int(month_num_str), day_num)
                            
                            # Process first shift (Row 1 and Row 2)
                            start_time_str1 = shift1_start_row[col_idx].strip() if len(shift1_start_row) > col_idx and shift1_start_row[col_idx] else None
                            end_time_str1 = shift1_end_row[col_idx].strip() if len(shift1_end_row) > col_idx and shift1_end_row[col_idx] else None
                            
                            # Process second shift (Row 4 and Row 5)
                            start_time_str2 = shift2_start_row[col_idx].strip() if len(shift2_start_row) > col_idx and shift2_start_row[col_idx] else None
                            end_time_str2 = shift2_end_row[col_idx].strip() if len(shift2_end_row) > col_idx and shift2_end_row[col_idx] else None

                            # --- DEBUG LOGS: Print extracted time strings for each day and shift ---
                            print(f"DEBUG: Day {day_num}: Shift 1 Start='{start_time_str1}', End='{end_time_str1}'")
                            print(f"DEBUG: Day {day_num}: Shift 2 Start='{start_time_str2}', End='{end_time_str2}'")
                            # --- END DEBUG LOGS ---

                            # Function to add a shift if valid
                            def add_shift_if_valid(start_str, end_str, date_obj, shift_type_idx):
                                if (start_str and re.fullmatch(r"(\d{1,2}):(\d{2})", start_str) and
                                    end_str and re.fullmatch(r"(\d{1,2}):(\d{2})", end_str)):
                                    try:
                                        start_dt_obj = dt.datetime.fromisoformat(f"{date_obj.isoformat()}T{start_str}")
                                        end_dt_obj = dt.datetime.fromisoformat(f"{date_obj.isoformat()}T{end_str}")

                                        if end_dt_obj < start_dt_obj:
                                            end_dt_obj += dt.timedelta(days=1)

                                        key = f"{date_obj:%Y%m%d}-{shift_type_idx}-{start_str.replace(':','')}-{end_str.replace(':','')}"
                                        shifts.append({
                                            "key": key,
                                            "date": date_obj.isoformat(),
                                            "start": start_str,
                                            "end": end_str
                                        })
                                        print(f"DEBUG: Added shift for {date_obj.isoformat()} from {start_str} to {end_str} (Key: {key})")
                                    except ValueError as ve:
                                        app.logger.warning(f"    Error parsing time for {date_obj.isoformat()} shift {shift_type_idx}: {ve}. Skipping.")
                                        print(f"DEBUG: ValueError for {date_obj.isoformat()} shift {shift_type_idx}: {ve}")
                                else:
                                    app.logger.info(f"    No valid shift times found for {date_obj.isoformat()} shift {shift_type_idx}. Skipping.")
                                    print(f"DEBUG: Invalid time format for {date_obj.isoformat()} shift {shift_type_idx}. Start: '{start_str}', End: '{end_str}'")

                            add_shift_if_valid(start_time_str1, end_time_str1, current_date, 1)
                            add_shift_if_valid(start_time_str2, end_time_str2, current_date, 2)
                            
                            current_day_counter += 1 # Increment day counter for the next iteration

                    # --- End of specific table data processing ---

    except Exception as e:
        app.logger.error(f"An error occurred while parsing the PDF: {e}")
        print(f"DEBUG: An unexpected error occurred during PDF parsing: {e}")
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
        return render_template("index.html", logged_in=False, error=f"Error de configuració de l'aplicació: {e}. Comproba les variables d'entorn.")
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
