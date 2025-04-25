"""Template for newly generated pipelines."""

from openhexa.sdk import current_run, pipeline, parameter


@pipeline("get_next_campaign_tog")
@parameter("country_id", name="identifier du pays", type=int, default=29710, required=False)
def get_next_campaign_tog(country_id: int):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    log_country_id(country_id)


# ------------
# tasks
# -----------

@get_next_campaign_tog.task
def log_country_id(country_id: int):
    """Put some data processing code here."""
    current_run.log_info(f"You want to get the next campaign for {country_id}")

#
# Main
#

if __name__ == "__main__":
    get_next_campaign_tog()