import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os

def extract_google_sheet_data():
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    GOOGLE_SPREADSHEET_ID = '1UqwIH4xfDOXip3_zGBK5HmARykgFYkwrhQ2nYMaTtoA'
    GOOGLE_RANGE_NAME = 'Influencer_Campaign_Data!A:C'
    
    try:
        creds = service_account.Credentials.from_service_account_file(
            'source/utils/service_account.json', scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=GOOGLE_SPREADSHEET_ID,
                                  range=GOOGLE_RANGE_NAME).execute()
        values = result.get('values', [])
        
        if not values:
            raise ValueError("No data found in Google Sheet")
            
        df = pd.DataFrame(values[1:], columns=values[0])
        
        # Save to temporary CSV
        temp_path = '/tmp/google_sheet_data.csv'
        df.to_csv(temp_path, index=False)
        return temp_path
        
    except Exception as e:
        raise Exception(f"Error extracting data from Google Sheet: {str(e)}")