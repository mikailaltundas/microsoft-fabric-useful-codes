#!/usr/bin/env python
# coding: utf-8

# ## lakehouse_delta_maintenance
#
# New notebook

# This notebook runs maintenance on every Delta table in the attached Lakehouse.
# It compacts small files with OPTIMIZE and then reclaims storage from old,
# unreferenced files with VACUUM. Running this regularly keeps queries fast and
# storage costs down, especially for tables that get many small incremental writes.
#
# Attach a Lakehouse to this notebook before running. The script reads the list of
# tables straight from the Spark catalog, so it picks up whatever is in the default
# Lakehouse. VACUUM permanently deletes old files: keep the retention window long
# enough that no in-progress reader or time-travel query still needs them.

# In[ ]:


# *** CONFIGURE THESE VALUES ***
# Run OPTIMIZE (small-file compaction) on each table
RUN_OPTIMIZE = True

# Run VACUUM (delete old, unreferenced data files) on each table
RUN_VACUUM = True

# Retention window for VACUUM, in hours. Default 168 (7 days) is the safe minimum.
# Going lower requires disabling a safety check and risks breaking time travel.
VACUUM_RETAIN_HOURS = 168

# Optional: only process these tables. Empty list = every table in the Lakehouse.
ONLY_TABLES = []
# *** END OF CONFIGURATION ***


# In[ ]:


def list_tables():
    """Return the names of tables in the default Lakehouse."""
    tables = [t.name for t in spark.catalog.listTables()]
    if ONLY_TABLES:
        tables = [t for t in tables if t in ONLY_TABLES]
    return tables


# In[ ]:


def optimize_table(table_name):
    """Compact small files for a single table."""
    spark.sql(f"OPTIMIZE `{table_name}`")
    print(f"  OPTIMIZE done: {table_name}")


def vacuum_table(table_name, retain_hours):
    """Remove old data files for a single table."""
    spark.sql(f"VACUUM `{table_name}` RETAIN {retain_hours} HOURS")
    print(f"  VACUUM done: {table_name} (retain {retain_hours}h)")


# In[ ]:


# Run maintenance
tables = list_tables()
print(f"Found {len(tables)} table(s) to process.\n")

succeeded = []
failed = []

for table_name in tables:
    print(f"Processing: {table_name}")
    try:
        if RUN_OPTIMIZE:
            optimize_table(table_name)
        if RUN_VACUUM:
            vacuum_table(table_name, VACUUM_RETAIN_HOURS)
        succeeded.append(table_name)
    except Exception as error:
        print(f"  Failed: {table_name} -> {error}")
        failed.append(table_name)


# In[ ]:


# Summary
print("\n================ Summary ================")
print(f"Succeeded: {len(succeeded)}")
print(f"Failed:    {len(failed)}")
if failed:
    print("Failed tables: " + ", ".join(failed))
