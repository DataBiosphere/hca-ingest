from dagster import failure_hook, success_hook, HookContext


@failure_hook(required_resource_keys={"slack"})
def notify_failure(context: HookContext):
    failure_details = [
        f"Failure Alert",
        f"Job: `{context.job_name}`",
        f"Op: `{context.op.name}`",
        f"Run ID: `{context.run_id}`",
    ]

    if context.op_exception:
        failure_details.append(f"Error: `{context.op_exception}`")

    failure_message = "\n".join(failure_details)

    context.resources.slack.send_message(failure_message)


@success_hook(required_resource_keys={"slack"})
def notify_success(context: HookContext):
    success_details = [f"Success Alert", f"Job: `{context.job_name}`", f"Op: `{context.op.name}`",
                       f"Run ID: `{context.run_id}`", "Status: `Completed Successfully`"]

    success_message = "\n".join(success_details)

    context.resources.slack.send_message(success_message)
