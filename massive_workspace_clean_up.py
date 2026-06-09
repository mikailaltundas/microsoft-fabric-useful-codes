#!/usr/bin/env python
# coding: utf-8

# ## massive_workspace_clean_up
#
# New notebook

# If you have multiple workspaces and need to delete all their content without
# deleting the workspaces themselves, list the workspace names in the
# WORKSPACES_TO_PURGE variable below and run the notebook.
#
# Safety: DRY_RUN is True by default, so the first run only reports what would
# be deleted without changing anything. Set DRY_RUN = False to actually delete.
#
# Notes:
# - Standalone semantic models can now be deleted through the Fabric API, so
#   they are deleted by default. Set DELETE_SEMANTIC_MODELS = False to keep them.
#   Deleting a semantic model also removes the reports and dashboard tiles that
#   depend on it.
# - Auto-generated items such as SQL analytics endpoints cannot be deleted
#   directly; they are removed automatically when their parent lakehouse or
#   warehouse is deleted.
# - You need at least the Contributor role in each workspace.

# In[ ]:


import sempy.fabric as fabric

# Workspaces whose contents should be purged (the workspaces themselves are kept).
WORKSPACES_TO_PURGE = [
    "Workspace 1",
    "Workspace 2",
    "Workspace 3",
]

# Safety switch: when True, nothing is deleted and the script only reports what
# it would do. Set to False to perform the deletions.
DRY_RUN = True

# When True, semantic models are deleted as well. Deleting a semantic model
# also deletes the reports and dashboard tiles that depend on it.
DELETE_SEMANTIC_MODELS = True

# Number of deletion passes. Several passes resolve dependencies between items,
# for example a report that must be removed before the model it relies on.
MAX_PASSES = 3


def get_workspace_id_by_name(workspace_name: str) -> str:
    """Return the workspace ID for an exact (case-sensitive) name match."""
    workspaces_df = fabric.list_workspaces()
    matching_df = workspaces_df[workspaces_df["Name"] == workspace_name.strip()]

    # Handle cases where no workspace or multiple workspaces are found.
    if matching_df.empty:
        raise ValueError(f"Workspace '{workspace_name}' not found.")
    if len(matching_df) > 1:
        raise ValueError(f"Multiple workspaces are named '{workspace_name}'.")

    return matching_df.iloc[0]["Id"]


def delete_all_items_in_workspace(
    workspace_id: str,
    workspace_name: str,
    delete_semantic_models: bool = True,
    dry_run: bool = True,
    max_passes: int = 3,
) -> dict:
    """Delete the items in a workspace and return per-workspace counts."""
    print(f"Workspace: {workspace_name} ({workspace_id})")
    counts = {"deleted": 0, "skipped": 0, "failed": 0}

    for pass_number in range(1, max_passes + 1):
        items_df = fabric.list_items(workspace=workspace_id)

        # Build the list of items to attempt this pass.
        pending = []
        for _, row in items_df.iterrows():
            item_type = str(row["Type"])
            if not delete_semantic_models and item_type.lower() == "semanticmodel":
                continue
            pending.append((row["Id"], row["Display Name"], item_type))

        if not pending:
            break

        made_progress = False
        for item_id, item_name, item_type in pending:
            if dry_run:
                print(f"  Would delete: {item_name} (Type: {item_type})")
                continue
            try:
                fabric.delete_item(item_id=item_id, workspace=workspace_id)
                print(f"  Deleted: {item_name} (Type: {item_type})")
                counts["deleted"] += 1
                made_progress = True
            except Exception as error:
                # Failures are expected on a first pass when items have
                # dependencies; a later pass may succeed.
                print(f"  Failed (pass {pass_number}): {item_name} ({item_type}): {error}")

        # In a dry run nothing changes, so one pass is enough. Otherwise stop
        # early once a full pass deletes nothing more.
        if dry_run or not made_progress:
            break

    # Report whatever survived, including intentionally skipped semantic models.
    remaining_df = fabric.list_items(workspace=workspace_id)
    for _, row in remaining_df.iterrows():
        item_type = str(row["Type"])
        if not delete_semantic_models and item_type.lower() == "semanticmodel":
            print(f"  Skipped (semantic model): {row['Display Name']}")
            counts["skipped"] += 1
        elif not dry_run:
            print(f"  Not deleted: {row['Display Name']} (Type: {item_type})")
            counts["failed"] += 1

    print(
        f"  Summary: {counts['deleted']} deleted, "
        f"{counts['skipped']} skipped, {counts['failed']} not deleted.\n"
    )
    return counts


# In[ ]:


totals = {"deleted": 0, "skipped": 0, "failed": 0}

if DRY_RUN:
    print("DRY RUN: no items will be deleted. Set DRY_RUN = False to delete.\n")

for workspace_name in WORKSPACES_TO_PURGE:
    try:
        workspace_id = get_workspace_id_by_name(workspace_name)
        counts = delete_all_items_in_workspace(
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            delete_semantic_models=DELETE_SEMANTIC_MODELS,
            dry_run=DRY_RUN,
            max_passes=MAX_PASSES,
        )
        for key in totals:
            totals[key] += counts[key]
    except ValueError as error:
        print(f"Skipping workspace '{workspace_name}': {error}\n")
    except Exception as error:
        print(f"Unexpected error in workspace '{workspace_name}': {error}\n")

print(
    f"All workspaces processed. Totals: {totals['deleted']} deleted, "
    f"{totals['skipped']} skipped, {totals['failed']} not deleted."
)