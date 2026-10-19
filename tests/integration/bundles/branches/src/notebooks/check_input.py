# Databricks notebook source
rows = int(dbutils.widgets.get("rows"))
dbutils.jobs.taskValues.set(key="rows", value=rows)
dbutils.notebook.exit(f"rows={rows}")
