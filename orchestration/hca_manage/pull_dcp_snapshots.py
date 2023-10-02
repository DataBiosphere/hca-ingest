"""Outputs the snapshots for a particular DCP release

Usage:
    > python3 pull_dcp_snapshots.py -r dcp_release"""

# Imports
import argparse

import data_repo_client  # type: ignore[import]
import google.auth  # type: ignore[import]
import google.auth.transport.requests  # type: ignore[import]
import pandas as pd  # type: ignore[import]
import requests  # type: ignore[import]


# Function to return the objects in a staging area bucket
def get_snapshots(release: str) -> pd.DataFrame:
    try:
        # Establish TDR API client
        creds, project = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        config = data_repo_client.Configuration()
        config.host = "https://data.terra.bio"
        config.access_token = creds.token
        api_client = data_repo_client.ApiClient(configuration=config)
        api_client.client_side_validation = False

        # Enumerate snapshots
        snapshot_filter = "_" + release
        snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
        snapshots_list = snapshots_api.enumerate_snapshots(filter=snapshot_filter, limit=1000)
        records_list = []
        for snapshot_entry in snapshots_list.items:
            public_flag = "N"
            public_response = requests.get(
                url="https://sam.dsde-prod.broadinstitute.org/api/resources/v2/datasnapshot/" +
                    f"{snapshot_entry.id}/policies/reader/public",
                headers={"Authorization": f"Bearer {creds.token}"},
            )
            if public_response.text == "true":
                public_flag = "Y"
            record = [
                snapshot_entry.id,
                snapshot_entry.name,
                snapshot_entry.data_project,
                snapshot_entry.created_date[0:10],
                snapshot_entry.created_date,
                public_flag
            ]
            records_list.append(record)
        df = pd.DataFrame(
            records_list,
            columns=[
                "TDR Snapshot ID",
                "TDR Snapshot Name",
                "TDR Snapshot Google Project",
                "Created Date",
                "Created Datetime",
                "Public Flag"
            ]
        )
        df_sorted = df.sort_values(by=["TDR Snapshot Name"], ignore_index=True)
    except Exception as e:
        print(f"Error retrieving snapshots: {str(e)}")
        df_sorted = pd.DataFrame()
    return df_sorted


#  Main function
if __name__ == "__main__":

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Pull snapshots for a particular DCP release.")
    parser.add_argument("-r", "--release", required=True, type=str, help="DCP release code (e.g., dcp25).")
    args = parser.parse_args()

    # Call functions to identify and remove outdated entity files
    print(f"Pulling snapshots for release: {args.release}")
    df = get_snapshots(args.release)
    file_name = f"dcp_snapshot_list_{args.release}.tsv"
    df.to_csv(file_name, sep="\t")
    print(f"Results outputed to {file_name}")
