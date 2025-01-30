#!/usr/bin/env python
# coding: utf-8

# ## massive_workspace_clean_up
# 
# New notebook

# If you have multiple workspaces and need to delete all their content without deleting the workspaces themselves, enter the names of the workspaces to be purged in the **WORKSPACES_TO_PURGE** variable and run the script.
# 
# Note: As of January 2025, semantic models cannot be deleted automatically. They must be removed manually.

# In[ ]:


import sempy.fabric as fabric

# List of workspaces to purge
WORKSPACES_TO_PURGE = ["Workspace 1", "Workspace 2", "Workspace 3", "..."]

def get_workspace_id_by_name(workspace_name: str) -> str:
    """Retrieve the workspace ID by its name."""
    
    # Fetch the list of available workspaces
    workspaces_df = fabric.list_workspaces()
    
    # Filter the workspaces by the provided name
    matching_df = workspaces_df[workspaces_df["Name"] == workspace_name]

    # Handle cases where no workspace or multiple workspaces are found
    if matching_df.empty:
        raise ValueError(f"Workspace '{workspace_name}' not found.")
    if len(matching_df) > 1:
        raise ValueError(f"Multiple workspaces named '{workspace_name}'.")
    
    # Return the workspace ID
    return matching_df.iloc[0]["Id"]

def delete_all_items_in_workspace(workspace_id: str) -> None:
    """Delete all items in the specified workspace except Semantic Models."""
    
    # Fetch the list of items in the workspace
    items_df = fabric.list_items(workspace=workspace_id)
    print(f"Starting deletion process for workspace: {workspace_id} ...")

    # Iterate over each item in the workspace
    for _, row in items_df.iterrows():
        item_id = row["Id"]
        item_name = row["Display Name"]
        item_type = row["Type"].lower()

        # Skip deletion for Semantic Models
        if item_type == "semanticmodel":
            print(f"Skipped: {item_name} (Semantic Model)")
            continue

        # Attempt to delete the item
        try:
            fabric.delete_item(item_id=item_id, workspace=workspace_id)
            print(f"Deleted: {item_name} (Type: {item_type}, ID: {item_id})")
        except Exception as error:
            print(f"Failed to delete {item_name} ({item_type}, {item_id}): {error}")

    print(f"Finished deletion process for workspace: {workspace_id}\n")

if __name__ == "__main__":
    # Iterate over the list of workspaces to purge
    for workspace_name in WORKSPACES_TO_PURGE:
        try:
            # Retrieve the workspace ID
            workspace_id = get_workspace_id_by_name(workspace_name)
            
            # Delete all items in the workspace
            delete_all_items_in_workspace(workspace_id)
        except ValueError as error:
            print(f"Skipping workspace {workspace_name}: {error}")
        except Exception as error:
            print(f"Unexpected error occurred in {workspace_name}: {error}")

