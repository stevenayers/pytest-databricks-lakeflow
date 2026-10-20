# Databricks notebook source
item = dbutils.widgets.get("item")
if item == "set_value":
    # The platform refuses this in an iteration of a for-each task, so the iteration fails.
    dbutils.jobs.taskValues.set(key="seen", value=item)
if item.startswith("fail"):
    raise ValueError(f"iteration failed on {item}")
dbutils.notebook.exit(item)
