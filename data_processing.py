import json
import logging as log
import os
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer


def load_data_from_sheet(sheet_id):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    creds_json = os.getenv('GOOGLE_SHEETS_CRED')
    credentials = Credentials.from_service_account_info(json.loads(creds_json)).with_scopes(scope)

    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(sheet_id)
    sheet = spreadsheet.sheet1
    records = sheet.get_all_records()
    log.info('Odczytano dane z Google Sheets')
    return pd.DataFrame(records)


def fetch_data():
    data = pd.read_csv('data_student_25020.csv')
    return data


def clean_data(data):
    log.info("Rozpoczeto czyszczenie danych")

    df = pd.DataFrame(data)

    starting_rows = len(df)

    df.dropna(thresh=2)

    invalid_rows = df.apply(invalid_time, axis=1)
    df = df[~invalid_rows]

    columns_clear = starting_rows - len(df)

    log.info(f'Zakonczono czyszczenie danych. Usunieto {columns_clear} wierszy')

    log.info('Rozpoczęto uzupełnianie danych')

    df_before_changed = df.copy()

    imputer_num = SimpleImputer(strategy='mean')
    imputer_cat = SimpleImputer(strategy='most_frequent')

    df[['Wiek', 'Średnie Zarobki']] = df[['Wiek', 'Średnie Zarobki']].replace('', pd.NA)
    df[['Płeć', 'Wykształcenie', 'Cel Podróży']] = df[['Płeć', 'Wykształcenie', 'Cel Podróży']].replace('', np.nan)

    df['Wiek'] = pd.to_numeric(df['Wiek'])
    df['Średnie Zarobki'] = pd.to_numeric(df['Średnie Zarobki'])

    df[['Wiek', 'Średnie Zarobki']] = imputer_num.fit_transform(df[['Wiek', 'Średnie Zarobki']])
    df[['Płeć', 'Wykształcenie', 'Cel Podróży']] = imputer_cat.fit_transform(
        df[['Płeć', 'Wykształcenie', 'Cel Podróży']])

    changed_rows = (df[['Wiek', 'Średnie Zarobki']] != df_before_changed[['Wiek', 'Średnie Zarobki']])
    changed_rows |= (df[['Płeć', 'Wykształcenie', 'Cel Podróży']] != df_before_changed[
        ['Płeć', 'Wykształcenie', 'Cel Podróży']])

    log.info(f'Zakonczono uzupełnianie danych. Zmieniono {columns_clear - changed_rows.sum().sum()} wierszy')

    log.info('Rozpoczęto standaryzacje danych')

    scaler = StandardScaler()

    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

    log.info('Zakonczono standaryzacje')

    generate_report(starting_rows, columns_clear, columns_clear, changed_rows)

    return df


def invalid_time(row):
    if pd.isna(row['Czas Początkowy Podróży']) or pd.isna(row['Czas Końcowy Podróży']):
        return True
    try:
        start_hour, start_minute = map(int, row['Czas Początkowy Podróży'].split(':'))
        end_hour, end_minute = map(int, row['Czas Końcowy Podróży'].split(':'))
    except ValueError:
        return True

    if start_hour > 24:
        return True

    duration = (end_hour - start_hour) % 24
    if duration > 12:
        return True

    return False


def generate_report(before, columns_clear, before_changed, changed_rows):
    report = f"Procent usunietych danych: {columns_clear / before * 100:.2f}%"
    report += f'\nProcent zmienionych danych: {changed_rows.sum().sum() / before_changed * 100:.2f}%'

    with open('report.txt', 'w') as f:
        f.write(report)

    log.info("Wygenerowanie raportu")


if __name__ == '__main__':
    log.basicConfig(
        level=log.INFO,
        handlers=[
            log.FileHandler("log.txt", mode='w'),
            log.StreamHandler()
        ]
    )
    data = load_data_from_sheet('15TcycXle2xMR6MZn4cY84bjstEJNnfgz15XuCX4GMAo')
    cleaned_data = clean_data(data)
    cleaned_data.to_csv('cleaned_data.csv', index=False)
    log.info("Zapisano wyczyszczone dane do pliku csv")
