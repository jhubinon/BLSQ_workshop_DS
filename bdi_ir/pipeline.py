"""Burundi data extraction"""
"""Extract monthly data for input variables, at the specified level of aggregation"""

#%% Imports

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from openhexa.sdk import pipeline, current_run, workspace, parameter
from openhexa.sdk.workspaces.connection import DHIS2Connection

import os
import polars as pl
from datetime import datetime
from types import SimpleNamespace

#%% Paths

root_path = f"{workspace.files_path}"
project_path = os.path.join(root_path, 'bdi_ir')
# code_path = os.path.join(project_path, 'code')
output_path = os.path.join(project_path, 'outputs')

#%% Utilities

def append_api_to_url(url: str) -> str:
    """
    Add '/api' suffix to the input url, if it doesn't already contain it
    Parameters:
        - url: the url of the connection
    Returns:
        - the url with "api" suffix
    """

    url = url.rstrip("/")
    if "/api" not in url:
        url += "/api"
    return url

class Session:
    def __init__(self, connection_id):
        self.session = requests.Session()
        self.authenticate(connection_id)

    def authenticate(self, con):
        con = workspace.dhis2_connection(con)
        self.url = append_api_to_url(con.url)
        self.session.auth = requests.auth.HTTPBasicAuth(con.username, con.password)

    def get(self, endpoint: str, params: dict = None) -> dict:
        """Send GET request and return JSON response as a dict."""
        r = self.session.get(f"{self.url}/{endpoint}", params=params)
        if r.status_code == 200:
            print("Yay, GET request successful!")
        else:
            print(f"Boo, GET request failed: {r.status_code}")
        return r.json()

#%% Helper functions

def format_varlist(vars:str) -> list:
    """
    Formats a string of variable names as a list
    """
    vars = vars.strip()
    vars = vars.replace(" ", "")
    varlist = vars.split(",")

    return varlist

def generate_monthly_periods(year_begin:int, year_end:int, month_begin:int, month_end:int) -> list:
    """
    Makes a list of integers of type YYYYMM for the periods to extract
    Parameters:
        - year_begin: year of the first period to extract
        - year_end: year of the last period to extract
        - month_begin: month of the first period to extract
        - month end: month of the last period to extract
    Returns:
        - a list of all the YYYYMM periods between the first and the last (closed interval)
    """
    periods = []
    for year_in_period in range(year_begin, year_end + 1):
        start_month = month_begin if year_in_period == year_begin else 1
        end_month = month_end if year_in_period == year_end else 12
        for month in range(start_month, end_month + 1):
            periods.append(f"{year_in_period}{month:02d}")
    return periods

def make_extraction_dimlist(dx_list:list, pe_list:list, ou_level:str) -> list:
    """
    Formats the dimensions to extract as a list
    Parameters:
        - dx_list: list of all the data elements/indicators/variables to extract
        - pe_list: list of all the monthly periods to extract
        - ou_level: the administrative level of the extraction
    Returns:
        - ordered list of the dimensions
    """
    extraction_dx = "dx:" + ';'.join(dx_list)
    extraction_pe = "pe:" + ';'.join(pe_list)
    extraction_ou = "ou:" + ou_level
    extraction_dimlist = [extraction_dx, extraction_pe, extraction_ou]
    return extraction_dimlist

def get_colnames(data_extraction):
    """
    Create a list of column names from the id's of the indicators extracted
    """
    columns = [header["name"] for header in data_extraction["headers"]]
    return columns

#%% Pipeline definition

@pipeline("bdi_ir")

@parameter(
    "extraction_vars",
    name="Variables separated by commas",
    type=str,
    default="nAQnroqvf3T,B9KO90o3CSH",
    required=True
)
@parameter(
    "extraction_year_begin",
    name="Beginning year",
    type=int,
    default=2023,
    required=True
)
@parameter(
    "extraction_year_end",
    name="End year",
    type=int,
    default=2024,
    required=True
)
@parameter(
    "extraction_month_begin",
    name="Beginning month",
    type=int,
    default=2,
    required=True
)
@parameter(
    "extraction_month_end",
    name="End month",
    type=int,
    default=10,
    required=True
)
@parameter(
    "extraction_admin_level",
    name="Administrative level",
    type=str,
    default='LEVEL-NJZ7J4g91OS',
    required=True
)
@parameter(
    "connection_id",
    name="DHIS2 Connection ID",
    type=str,
    default="iulia-bdi",
    required=True
)

def bdi_ir(
    extraction_vars: str,
    extraction_year_begin: int,
    extraction_month_begin: int,
    extraction_year_end: int,
    extraction_month_end: int,
    extraction_admin_level: str,
    connection_id: str
):
    
    # Format the variables to be extracted
    extraction_varlist = format_varlist(extraction_vars)
    current_run.log_info(f"Variables to extract: {', '.join(extraction_varlist)}")
    
    # Format the periods to be extracted
    extraction_period_list = generate_monthly_periods(
        extraction_year_begin,
        extraction_year_end,
        extraction_month_begin,
        extraction_month_end
    )
    current_run.log_info(f"Periods to extract: {', '.join(extraction_period_list)}")
    
    # Spatial/administrative level of the extraction
    current_run.log_info(f"Administrative level to extract: {extraction_admin_level}")

    # Bring the dimensions together
    extraction_dims = make_extraction_dimlist(
        extraction_varlist,
        extraction_period_list,
        extraction_admin_level
    )
    # print(f"Extracting dimensions: {extraction_dims}")

    session = Session(connection_id)

    extraction_data_dict = session.get(
        endpoint='analytics',
        params={"dimension": extraction_dims}
    )

    extraction_colnames = get_colnames(extraction_data_dict)

    long_df = pl.DataFrame(extraction_data_dict["rows"], schema=extraction_colnames, orient='row')

    current_run.log_info("Received data in long format")
    
    output_df = long_df.pivot(
        on='dx',
        values='value'
        )

    filename = f"{connection_id}.csv"

    full_output_path = os.path.join(output_path, filename)

    output_df.write_csv(full_output_path)


#%% Run locally
if __name__ == "__main__":
    # Only runs locally
    bdi_ir(
        extraction_vars="nAQnroqvf3T,B9KO90o3CSH",
        extraction_year_begin=2023,
        extraction_month_begin=2,
        extraction_year_end=2024,
        extraction_month_end=10,
        extraction_admin_level="LEVEL-NJZ7J4g91OS",
        connection_id="iulia-bdi"
    )
