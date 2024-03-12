import gspread
import google.auth
import pandas as pd
import gspread_dataframe as gd
from google.cloud import bigquery


def main(event, context):

    sheets_id = '1Q6unwl3nqjJ1BZgbPCnJt-GUBuUOVM4Hjv0NoikfW4o'
    colum_headers = ['Size', 'Weight']
    
    DEFAULT_SCOPES =['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
    credentials, project_id = google.auth.default(scopes=DEFAULT_SCOPES)

    keyfile = gspread.authorize(credentials)

    # df = extract_all_sheets(sheets_id=sheets_id, keyfile=keyfile)

    df = extract_specific_sheets(sheets_id=sheets_id, column_headers=colum_headers, keyfile=keyfile)

    load_bq(dataframe=df, table_name='fruits_')

def extract_specific_sheets(sheets_id, column_headers, keyfile):

    sheet_open = keyfile.open_by_key(sheets_id)

    sheets_tab = ['base']

    dfs = []

    for sheet in sheets_tab:

        df = gd.get_as_dataframe(sheet_open.worksheet(sheet))
        df = df[column_headers]
        dfs.append(df)
        
    all_df = pd.concat(dfs)
    
    all_df = all_df.dropna()

    all_df.columns = map(str.lower, all_df.columns)

    return all_df


def extract_all_sheets(sheets_id, keyfile):

    sheets_open = keyfile.open_by_key(sheets_id)

    sheets_tab = 'base'

    sheet_base = sheets_open.worksheet(sheets_tab)

    values = sheet_base.get_all_values()

    columns = values[0]

    all_df = pd.DataFrame(values[1:], columns=columns)
    
    all_df = all_df.dropna()

    all_df.columns = map(str.lower, all_df.columns)

    return all_df

def load_bq(dataframe, table_name):

    bigquery_client = bigquery.Client()

    dataset_id = 'aula1'

    table_ref = bigquery_client.dataset(dataset_id).table(table_name)

    try:
        table = bigquery_client.create_table(table_ref)
    except:
        table = bigquery_client.get_table(table_ref)

    bigquery_client.load_table_from_dataframe(dataframe, table)

    print("tabela importada "+table_name)
