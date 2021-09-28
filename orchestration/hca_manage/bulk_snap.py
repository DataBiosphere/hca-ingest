import csv
import re
from common import get_api_client, data_repo_host


def run():
    jade = get_api_client(host=data_repo_host['dev'])
    with open("../sorted_snapshot_names.csv", "r") as f:
        reader = csv.reader(f)
        for row in reader:
            snapshot_name = row[0]
            response = jade.enumerate_snapshots(filter=snapshot_name)
            latest_snapshot = sorted(response.items, key=lambda snapshot: snapshot.created_date, reverse=True)[0]
            real_latest = jade.retrieve_snapshot(id=latest_snapshot.id, include=["PROFILE,DATA_PROJECT"])
            ds_name = real_latest.name[:real_latest.name.rindex('_')]

            print(f"{ds_name}\t{real_latest.name}\t{real_latest.data_project}")


if __name__ == '__main__':
    run()
