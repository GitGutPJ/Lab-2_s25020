import json
import os
import gspread
from google.oauth2.service_account import Credentials


def upload(sheet_id):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    creds_json = os.getenv('GOOGLE_SHEETS_CRED')
    credentials = Credentials.from_service_account_info(json.loads(creds_json)).with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(sheet_id)

    with open('data_student_25020.csv') as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet.id, data=content)


if __name__ == '__main__':
    upload('15TcycXle2xMR6MZn4cY84bjstEJNnfgz15XuCX4GMAo')
