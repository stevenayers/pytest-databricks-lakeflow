# Databricks notebook source
import time

# Job parameters arrive as widgets. Catalog and schema must come from job parameters, not bundle variables,
# so that each test can point the job at its own temporary schema.
param1 = dbutils.widgets.get("param1")
param2 = dbutils.widgets.get("param2")
schema = dbutils.widgets.get("schema")
mode = dbutils.widgets.get("mode")
row_count = int(dbutils.widgets.get("row_count"))
source_path = dbutils.jobs.taskValues.get(taskKey="extract", key="source_path")

# COMMAND ----------

# The printed lines reach the test through task.notebook.stdout when the task fails.
print(f"transform: checking {param1} against {source_path}")
print(f"transform: {row_count} rows expected")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="stage", value="validation")
dbutils.jobs.taskValues.set(key="source_path", value=source_path)
if param1 == "forbidden":
    raise PermissionError(f"{param1} may not read {source_path}")
if param1 == "explode":
    raise ValueError(f"validation failed for {param1}")
if param1 == "chained":
    # A wrapped exception: error.root_cause should name the KeyError, not the RuntimeError.
    raise RuntimeError("wrapper failed") from KeyError("root cause")

# COMMAND ----------

if schema:
    spark.range(row_count).write.mode("overwrite").saveAsTable(f"{schema}.example_rows")
    if mode == "full":
        # A full load is deliberately slow so that the example tests can compare run durations.
        time.sleep(30)

dbutils.jobs.taskValues.set(key="rows_written", value=row_count)
dbutils.jobs.taskValues.set(key="stage", value="done")
dbutils.notebook.exit(f"{param1} {param2}")
