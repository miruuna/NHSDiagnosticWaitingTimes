import os
import argparse

import requests
from bs4 import BeautifulSoup
import pandas as pd

from azure.storage.blob import  BlobClient

conn_string = os.environ.get("BLOB_CONNECTION_STRING")


def get_link(year, month):
    code_after_year = int(year%2000)+1
    url = f'https://www.england.nhs.uk/statistics/statistical-work-areas/diagnostics-waiting-times-and-activity/monthly-diagnostics-waiting-times-and-activity/monthly-diagnostics-data-{year}-{code_after_year}/'
    
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    links = soup.find_all('a', href=True)
    for link in links:
        if "Monthly-Diagnostics-Web-File-Provider" in link["href"] and month in link["href"]:
            file_link = str(link["href"])
            print("Downloading file from url:", file_link)
            return file_link

def download_file(year, month):
    """
    Download file locally and return name of the file
    """
    file_url = get_link(year, month)
    filename = f'diagnostics_{year}_{month}.xlsx'

    response = requests.get(file_url)
    
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"File saved as: {filename}")
        return filename
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

def get_monthly_df(year, month):
    file_name = download_file(year, month)
    
    ## Clean data ##

    df = pd.read_excel(file_name, 'Provider by Test')
    df = df.drop(columns=['Unnamed: 0', 'Unnamed: 1'])

    # Slice the DataFrame from that index onward
    new_df = df.loc[12:].reset_index(drop=True)
    new_df = new_df.dropna(how='all')

    # Remove row with total numbers
    new_df = new_df[new_df['Unnamed: 4'] != "Total"]

    # Make first row header
    new_df.columns = new_df.iloc[0] 
    new_df = new_df[1:]             
    new_df = new_df.reset_index(drop=True)

    values_pivot = new_df.pivot_table(index='Regional Team Name', columns='Diagnostic Test Name', values='Number waiting 6+ Weeks',aggfunc='sum')
    percentage_per_category = values_pivot.divide(values_pivot.sum(axis=0), axis=1) * 100
    percentage_per_category= percentage_per_category.reset_index()

    melted_df = pd.melt(percentage_per_category, id_vars='Regional Team Name', value_vars=percentage_per_category.columns.tolist(), var_name='Diagnostic Test Name', value_name='+6weeks(%)')
    melted_df['+6weeks(%)'] = melted_df['+6weeks(%)'].apply(lambda x: f"{x:.2f}")

    melted_df["Year"] = year
    melted_df["Month"] = month

    return melted_df


def upload_data(year, month):
    """
    Upload the file to an Azure blob and delete the local file
    """

    df = get_monthly_df(year, month)
    csv_data = df.to_csv(index=False)

    container_name = 'waitingtimestats'

    blob_client = BlobClient.from_connection_string(
        conn_string,
        container_name=container_name,
        blob_name=f"stats_{year}_{month}.csv",
    )

    # Open a local file and upload its contents to Blob Storage
    blob_client.upload_blob(csv_data)
    print("File uploaded to Azure Storage")

    file_path = f"stats_{year}_{month}.csv"
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"{file_path} has been deleted.")


def main():
    parser = argparse.ArgumentParser(description="Greet someone from the command line.")

    parser.add_argument("-y", "--year", type=int, help="Year represented as four digits - e.g. 2025")
    parser.add_argument("-m", "--month", type=str, help="Month represented as a string, staring with a capital letter - e.g. 'April'")

    
    args = parser.parse_args()

    upload_data(args.year, args.month)


if __name__ == "__main__":
    main()