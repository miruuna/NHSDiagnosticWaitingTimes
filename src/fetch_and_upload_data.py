import os
import argparse
from dotenv import load_dotenv

import requests
from bs4 import BeautifulSoup
import pandas as pd

from azure.storage.blob import BlobClient


load_dotenv()

conn_string = os.environ.get("BLOB_CONNECTION_STRING")
container_name = os.environ.get("CONTAINER_NAME")


def get_link(year, month):
    code_after_year = int(year % 2000) + 1
    url = f"https://www.england.nhs.uk/statistics/statistical-work-areas/diagnostics-waiting-times-and-activity/monthly-diagnostics-waiting-times-and-activity/monthly-diagnostics-data-{year}-{code_after_year}/"

    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("a", href=True)
    for link in links:
        if (
            "Monthly-Diagnostics-Web-File-Provider" in link["href"]
            and month in link["href"]
        ):
            file_link = str(link["href"])
            print("Downloading file from url:", file_link)
            return file_link


def download_file(year, month):
    """
    Download file locally and return name of the file
    """
    file_url = get_link(year, month)
    filename = f"diagnostics_{year}_{month}.csv"

    response = requests.get(file_url)

    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"File saved as: {filename}")
        return filename
    else:
        print(f"Failed to download file. Status code: {response.status_code}")


def get_monthly_df(year, month):
    file_name = download_file(year, month)

    ## Clean data ##

    df = pd.read_excel(file_name, "Provider by Test")
    df = df.drop(columns=["Unnamed: 0"])

    # Slice the DataFrame from that index onward
    cleaned_df = df.loc[12:].reset_index(drop=True)
    cleaned_df = cleaned_df.dropna(how="all")

    # Remove row with total numbers
    cleaned_df = cleaned_df[cleaned_df["Unnamed: 4"] != "Total"]
    cleaned_df = cleaned_df[cleaned_df["Unnamed: 6"] != "Total"]

    # Make first row header
    cleaned_df.columns = cleaned_df.iloc[0]
    cleaned_df = cleaned_df[1:]
    cleaned_df = cleaned_df.reset_index(drop=True)

    cleaned_df = cleaned_df[
        cleaned_df["Total Waiting List"].notna()
        & (cleaned_df["Total Waiting List"] != 0)
    ]

    cleaned_df["+6weeks(%)"] = (
        cleaned_df["Number waiting 6+ Weeks"] / cleaned_df["Total Waiting List"] * 100
    )
    cleaned_df["+6weeks(%)"] = cleaned_df["+6weeks(%)"].apply(lambda x: f"{x:.2f}")

    cleaned_df = cleaned_df[
        [
            "Regional Team Code",
            "Regional Team Name",
            "Provider Code",
            "Provider Name",
            "Diagnostic ID",
            "Diagnostic Test Name",
            "+6weeks(%)",
        ]
    ]

    cleaned_df = cleaned_df.rename(
        columns={
            "Regional Team Code": "region_code",
            "Regional Team Name": "region_name",
            "Provider Code": "provider_code",
            "Provider Name": "provide_name",
            "Diagnostic ID": "diagnostic_id",
            "Diagnostic Test Name": "diagnostic_test_name",
            "+6weeks(%)": "percentage_over_6weeks",
        }
    )

    cleaned_df["year"] = year
    cleaned_df["month"] = month

    return cleaned_df


def upload_data(year, month):
    """
    Upload the file to an Azure blob and delete the local file
    """
    print("container name", conn_string)
    df = get_monthly_df(year, month)
    csv_data = df.to_csv(index=False)

    blob_client = BlobClient.from_connection_string(
        conn_string,
        container_name=container_name,
        blob_name=f"stats_{year}_{month}.csv",
    )

    # Open a local file and upload its contents to Blob Storage
    blob_client.upload_blob(csv_data, overwrite=True)
    print("File uploaded to Azure Storage")

    file_path = f"stats_{year}_{month}.csv"
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"{file_path} has been deleted.")


def main():
    parser = argparse.ArgumentParser(description="Greet someone from the command line.")

    parser.add_argument(
        "-y", "--year", type=int, help="Year represented as four digits - e.g. 2025"
    )
    parser.add_argument(
        "-m",
        "--month",
        type=str,
        help="Month represented as a string, staring with a capital letter - e.g. 'April'",
    )
    parser.add_argument(
        "--batch", type=str, help="Upload the data for an entire year: 'y' or 'n'"
    )

    args = parser.parse_args()
    if args.batch == "y":
        for month in [
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
            "January",
            "February",
            "March",
        ]:
            upload_data(args.year, month)
    else:
        upload_data(args.year, args.month)


if __name__ == "__main__":
    main()
