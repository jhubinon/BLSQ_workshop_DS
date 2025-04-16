"""Template for newly generated pipelines."""

# ------------------------------
# Imports
# ------------------------------
from openhexa.sdk import current_run, pipeline, parameter, workspace
import requests
import pandas as pd
import datetime


# -------------------------------
# Pipeline
# -------------------------------
@pipeline("get_next_campaign_JP")
@parameter(
    "country_id",
    name="Identifiant du pays sur poliooutbreaks",
    type=int,
    required=True,
)
@parameter(
    "iaso_polio_slug",
    name="Identifiant de la connexion",
    type=str,
    required=True,
)
def get_next_campaign_jp(country_id: int, iaso_polio_slug: str):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    log_country_id(country_id)
    headers = get_connection(iaso_polio_slug)
    campaign_info = get_next_campaign(country_id, headers)
    log_next_campaign(campaign_info)


# -------------------------------
# Tasks
# -------------------------------


@get_next_campaign_jp.task
def log_country_id(country_id: int):
    """Put some data processing code here."""
    current_run.log_info(f"You want to get the next campaign for {country_id}")


@get_next_campaign_jp.task
def get_connection(slug):
    connection = workspace.custom_connection(slug)
    IASO_POLIO_USERNAME = connection.username
    IASO_POLIO_PASSWORD = connection.password
    url = connection.url
    creds = {"username": IASO_POLIO_USERNAME, "password": IASO_POLIO_PASSWORD}
    r = requests.post(f"{url}/api/token/", json=creds)
    token = r.json().get("access")
    headers = {"Authorization": "Bearer %s" % token}
    return headers


@get_next_campaign_jp.task
def get_next_campaign(country_id: int, headers: dict):
    today = datetime.date.today()
    obr, round, start, vaccine = None, None, None, None
    closest = today + datetime.timedelta(days=31)
    r = requests.get(
        f"https://www.poliooutbreaks.com/api/polio/campaigns/?campaign_types=polio&country__id__in={country_id}",
        headers,
    )
    # &rounds__started_at__gte={today.strftime('%d/%m/%Y').replace('/','%2F')}"
    camps = r.json()
    if type(camps) == list and len(camps) > 0:
        # print(camps[0])
        for c in camps:
            for r in c["rounds"]:
                if type(r["started_at"]) == str:
                    date = datetime.datetime.strptime(r["started_at"], "%Y-%m-%d")
                    start_date = datetime.date(date.year, date.month, date.day)
                    if start_date >= today and start_date < closest:
                        obr, round, start, vaccine = (
                            c["obr_name"],
                            r["number"],
                            r["started_at"],
                            c["vaccines"],
                        )
                        closest = start_date
    return (obr, round, start, vaccine)


@get_next_campaign_jp.task
def log_next_campaign(campaign_info):
    """Put some data processing code here."""
    obr, round, start, vaccine = campaign_info
    if obr is not None:
        current_run.log_info(f"Next campaign for {obr} is round {round} on {start}")
        current_run.log_info(f"Vaccines: {vaccine}")
    else:
        current_run.log_warning("No campaign found in the next 30 days.")


# --------------------------------
# Main
# --------------------------------

if __name__ == "__main__":
    get_next_campaign_jp()
