# Code for ETL operations on Country-GDP data

# Importing the required libraries.
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


def log_progress(message):
    time_format = "%Y-%h-%d-%H-%M-%S"
    now = datetime.now()
    timestamp = now.strftime(time_format)
    with open(log_file_path, "a") as file:
        file.write(timestamp+" : "+message+"\n")


def extract(url, table_attribs):
    # Get the page from the URL
    page = requests.get(url).text
    # Create the dataframe with the table_attribs as the columns
    df = pd.DataFrame(columns=table_attribs)
    # Create the BeautifulSoup instance
    data = BeautifulSoup(page, "html.parser")
    # Find all the table bodies.
    tables = data.find_all("tbody")
    # From the web page it is evident that the first table is what we need, so it should be tables[0]
    table_of_interest = tables[0]
    # Now find all the table rows from the first table, which is containing all the banks and their market caps, according to the webpage.
    rows = table_of_interest.find_all("tr")
    # Now for every row in rows, find all the table data td
    for row in rows:
        cols = row.find_all("td")
        if len(cols) > 1:
            col_bank = cols[1]
            col_mcap = cols[2]
            bank_name = col_bank.find_all("a")[1].contents[0]
            mcap = col_mcap.contents[0]
            # Now this remove the last character from mcap and cast it to float
            mcap = np.round(float(mcap.strip()), 2)
            # Now create a dict with the resulting name and the market cap and append to the resulting dataframe
            dict = {
                "Name": bank_name,
                "MC_USD_Billion": mcap
            }
            df_temp = pd.DataFrame(dict, index=[0])
            df = pd.concat((df.dropna(axis=1, how='all') for df in [df, df_temp]), ignore_index=True)
    return df


def transform(df):
    # Read the rate CSV
    rates_df = pd.read_csv(exchangerate_file_path)
    rates_dict = rates_df.set_index('Currency').to_dict()['Rate']
    # Now create the new columns on the dataframe df and put the values into those columns upon
    # converting into the corresponding currencies. Also rounding the results to 2 decimal places.
    df["MC_GBP_Billion"] = [np.round(x * rates_dict["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * rates_dict["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * rates_dict["INR"], 2) for x in df["MC_USD_Billion"]]
    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_queries(query_statement, sql_connection):
    print(query_statement)
    output = pd.read_sql(query_statement, sql_connection)
    print(output)



def run_etl_process():
    log_progress("Preliminaries complete. Initiating ETL process")
    df = extract(url, table_attribs)
    log_progress("Data extraction complete. Initiating Transformation process")
    df = transform(df)
    log_progress("Data transformation complete. Initiating Loading process")
    load_to_csv(df, output_csv_path)
    log_progress("Data saved to CSV file")
    sql_connection = sqlite3.connect(db_name)
    log_progress("SQL Connection initiated")
    load_to_db(df, sql_connection, table_name)
    log_progress("Data loaded to Database as a table, Executing queries")
    run_queries(q_all, sql_connection)
    run_queries(q_avg, sql_connection)
    run_queries(q_top5_names, sql_connection)
    log_progress("Process complete")
    sql_connection.close()
    log_progress("Server Connection closed")

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/list_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
output_csv_path = "./Largest_banks_data.csv"
log_file_path = "./code_log.txt"
exchangerate_file_path = "./exchange_rate.csv"

# Queries
q_all = f"SELECT * FROM {table_name}"
q_avg = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
q_top5_names = f"SELECT Name FROM {table_name} LIMIT 5"

run_etl_process()

