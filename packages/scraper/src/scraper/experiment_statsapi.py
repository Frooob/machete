from prefect import flow, task
from prefect_aws.s3 import S3Bucket
import statsapi
import json

from prefect.cache_policies import TASK_SOURCE, INPUTS

from connector.setup_prefect import s3_buck_block_name

cache_policy = (TASK_SOURCE + INPUTS).configure(
    key_storage=S3Bucket.load(s3_buck_block_name)
)


@task(cache_policy=cache_policy)
def get_team_names():
    print("Getting data from the statsapi...")
    teams_names = [
        x["name"]
        for x in statsapi.get(
            "teams", {"sportIds": 1, "activeStatus": "Yes", "fields": "teams,name"}
        )["teams"]
    ]

    json_team_names = json.dumps({"names": teams_names})

    return json_team_names


@flow(log_prints=True)
def get_longest_team_name():
    team_names = json.loads(get_team_names())
    team_names = team_names["names"]
    team_names.sort(key=len, reverse=True)
    longest_team_name = team_names[0]

    return longest_team_name


if __name__ == "__main__":
    print("Starting the flow...")
    longest_team_name = get_longest_team_name()
    print(longest_team_name)
