import argparse
from data_repo_client import ApiException, PolicyMemberRequest

from hca_manage.common import get_api_client, data_repo_host

MANAGED_ACCESS_SNAPS = {
    "hca_dev_20210621_managedaccess_4298b4de92f34cbbbbfe5bc11b8c2422__20210622",
    "hca_dev_a004b1501c364af69bbd070c06dbc17d__20210830_20210903",
    "hca_dev_99101928d9b14aafb759e97958ac7403__20210830_20210903"""
}

AZUL_DEV_GROUP = "azul-dev@dev.test.firecloud.org"
AZUL_PUBLIC_GROUP = 'azul-public-dev@dev.test.firecloud.org'
MONSTER_HCA_DAGSTER_RUNNER = "hca-dagster-runner@broad-dsp-monster-hca-dev.iam.gserviceaccount.com"
MONSTER_DEV_GROUP = "monster-dev@dev.test.firecloud.org"


def run(dry_run=True):
    client = get_api_client(data_repo_host["dev"])
    enumerate_response = client.enumerate_snapshots(filter="hca_", limit=1000)
    for snapshot_model in enumerate_response.items:
        try:
            policies_response = client.retrieve_snapshot_policies(id=snapshot_model.id)
            for policy in policies_response.policies:
                if AZUL_PUBLIC_GROUP in policy.members:
                    print(
                        f"Should remove azul-public-dev from policy = {policy.name}, snapshot = {snapshot_model.name}")
                    if not dry_run:
                        print("Removing azul-public-dev from policy...")
                        response = client.delete_snapshot_policy_member(
                            id=snapshot_model.id, policy_name=policy.name, member_email=AZUL_PUBLIC_GROUP
                        )

                if snapshot_model.name in MANAGED_ACCESS_SNAPS and AZUL_DEV_GROUP not in policy.members and policy.name == 'reader':
                    print(f"Should add azul-dev to policy = {policy.name}, snapshot = {snapshot_model.name}")
                    if not dry_run:
                        print("Adding azul-dev to policy...")
                        payload = PolicyMemberRequest(email=AZUL_DEV_GROUP)
                        response = client.add_snapshot_policy_member(id=snapshot_model.id, policy_name="reader",
                                                                     policy_member=payload)

                if AZUL_DEV_GROUP in policy.members and snapshot_model.name not in MANAGED_ACCESS_SNAPS:
                    print(f"Should remove azul-dev from policy = {policy.name}, snapshot = {snapshot_model.name}")
                    if not dry_run:
                        print("Removing azul-dev from policy...")
                        response = client.delete_snapshot_policy_member(
                            id=snapshot_model.id, policy_name=policy.name, member_email=AZUL_DEV_GROUP
                        )

                if policy.name == 'steward' and MONSTER_DEV_GROUP not in policy.members:
                    print(f"Should add monster-dev to policy = {policy.name}, snapshot = {snapshot_model.name}")
                    if not dry_run:
                        print("Adding monster-dev to policy...")
                        payload = PolicyMemberRequest(email=MONSTER_DEV_GROUP)
                        response = client.add_snapshot_policy_member(id=snapshot_model.id, policy_name="steward",
                                                                     policy_member=payload)
        except ApiException as e:
            print(f"API exception while pulling policies for snapshot {snapshot_model.name}, skipping")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)

    args = parser.parse_args()
    run(args.dry_run)
