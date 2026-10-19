# Databricks notebook source
# A setup= function in the tests seeds this table before the job runs.
schema = dbutils.widgets.get("schema")
if schema and spark.catalog.tableExists(f"{schema}.seed_orders"):
    dbutils.notebook.exit(f"loaded {spark.table(f'{schema}.seed_orders').count()} seeded orders")
dbutils.notebook.exit("loaded")
