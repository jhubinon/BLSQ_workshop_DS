"""Template for newly generated pipelines."""

# ------------------------------
# Imports
# ------------------------------
from openhexa.sdk import current_run, pipeline, parameter


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
def get_next_campaign_jp(country_id: int):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    log_country_id(country_id)


# -------------------------------
# Tasks
# -------------------------------


@get_next_campaign_jp.task
def log_country_id(country_id: int):
    """Put some data processing code here."""
    current_run.log_info(f"You want to get the next campaign for {country_id}")


# --------------------------------
# Main
# --------------------------------

if __name__ == "__main__":
    get_next_campaign_jp()
