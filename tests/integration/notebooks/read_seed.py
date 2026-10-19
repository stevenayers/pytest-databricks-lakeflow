# The teardown notebook of test_sql_setup_reads_a_parameter_declared_by_modify_settings in test_example.py, passed as
# a Path. It has no "Databricks notebook source" line, so the plugin adds one.
import json

schema = dbutils.widgets.get("schema")
rows = spark.sql(f"SELECT * FROM {schema}.seed").collect()
dbutils.notebook.exit(json.dumps([row.asDict() for row in rows], default=str))
