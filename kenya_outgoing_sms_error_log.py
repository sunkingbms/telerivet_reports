from datetime import datetime, date, time as dt_time
import time
import telerivet

def get_failed_messages(API_KEY: str, ken_project_id: str, delay: int = 6) -> list:
    """
    Retrieve logs with a fail status and return an array of all the errors from the current day
    """
    messages = []
    current_date = date.today()

    # Create datetime objects for start/end of day
    start_of_day = datetime.combine(current_date, dt_time.min)
    end_of_day = datetime.combine(current_date, dt_time.max)
    
    # Convert to UTC timestamps
    start_timestamp = int(start_of_day.timestamp())
    end_timestamp = int(end_of_day.timestamp())

    tr = telerivet.API(API_KEY)
    project = tr.initProjectById(ken_project_id)

    try:
        cursor = project.queryMessages(
            direction="outgoing",
            message_type="sms",
            status="failed",
            time_created={"min": start_timestamp, "max": end_timestamp}
        )

        for message in cursor.all():
            # Convert timestamp to UTC datetime
            utc_time = datetime.utcfromtimestamp(message.time_sent)
            
            messages.append({
                "status": message.status,
                "error log": message.error_message,
                "from number": message.from_number,
                "to number": message.to_number,
                "time_sent": utc_time.strftime('%Y-%m-%d %H:%M:%S UTC')
            })

    except telerivet.APIException as e:
        print(f"API Error: {e}")
        time.sleep(delay)
    
    return messages

API_KEY = "w_n6I_mg7AINI7n2i3ejMHShsCPYVfevEvqo"
KEN_PROJECT_ID = "PJfea0e3ae2d40f54d"


def error_log_occurrence(data: list) -> dict:
    """
    Analyze error logs and return occurrence count of each error type.
    """
    try:
        if not data:
            print("Empty array passed")
            return {}
        error_log: dict = {}
        for error in data:
            error_message = error["error log"]
            error_log[error_message] = error_log.get(error_message, 0) + 1
        return error_log
    except Exception as e:
        print(f"Error occurred: {e}")
        return{}

"""
Author: Singa Maurice Shanga
Date: 11 March 2025
Version: 1.0
Description: 
    This script interacts with Google APIs using OAuth 2.0 authentication.
    It writes errors log details from the Telerivet API to a sheets designed
    to save the daily report.

License: MIT License

Dependencies:
    The following Python packages must be installed for this script to work:
    - google-auth
    - google-auth-oauthlib
    - google-auth-httplib2
    - google-api-python-client

Installation:
    To install the required dependencies, run:
    pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
"""
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from datetime import datetime


def save_to_sheets(data):
    """
        Save error logs to spreadsheet.
    """
  
    # The ID and range of a sample spreadsheet.
    SPREADSHEET_ID = "11hPihYyR1gFZ1UnJgy1yG2lUvToHegj8P4aKT_MMzwU"
    # Defining the scope as all actions available to Spreadsheets
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    # Path to the key created from the console cloud. 
    # In my case I have it in the same folder with the Python files
    SERVICE_ACCOUNT_FILE = 'Telerivet_Report_Key.json'
    RANGE="Kenya_Outgoing_SMS_Report!A2"


    creds = None
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
  
    try:
        service = build("sheets", "v4", credentials=creds)
        date_log = datetime.today().strftime("%Y-%m-%d")

        # Call the Sheets API
        sheet = service.spreadsheets()

        if not data:
            print("Error getting data!!")
            return
        # Converting the dictionary from telerivet to a list and prepare to be send
        # through Google Sheets API
        data_to_append = [[key, value, date_log] for key, value in data.items()]

        # Body to be passed to the Gogle API
        body = {
            "values": data_to_append
        }

        # Call the Google Sheets API to append the data from Telerivet function
        response = sheet.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
        print("Successfully written in the sheet!")

    except HttpError as err:
        print(err)

# Important keys to be deleted on Github
API_KEY = "w_n6I_mg7AINI7n2i3ejMHShsCPYVfevEvqo"
KEN_PROJECT_ID = "PJfea0e3ae2d40f54d"

# Colling all the functions written above

failed_messages = get_failed_messages(API_KEY, KEN_PROJECT_ID)
error_counts = error_log_occurrence(failed_messages)
saved_data = save_to_sheets(error_counts)

print(error_counts)
print(saved_data)

# Dependencies to be installed
# pip3 install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
# pip3 install telerivet