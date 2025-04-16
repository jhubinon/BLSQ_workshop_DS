
from openhexa.sdk import current_run, pipeline, parameter


@pipeline("get_next_campaign_GP")
@parameter("country_id", name = "Country ID", type = int, required = True)

def get_next_campaign_gp(country_id: int):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks, while tasks can perform IO operations like logging or computations.
    """
    log_country_id(country_id)


# ----------------------
# Tasks
#----------------------
@get_next_campaign_gp.task
def log_country_id(country_id: int):
    current_run.log_info(f"You want to get the next campaign for {country_id}")

if __name__ == "__main__":
    get_next_campaign_gp()