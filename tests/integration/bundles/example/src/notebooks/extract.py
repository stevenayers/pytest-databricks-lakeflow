# Databricks notebook source
dbutils.jobs.taskValues.set(key="row_count", value=7)
dbutils.jobs.taskValues.set(key="source_path", value="/Volumes/example/raw/landing")
dbutils.notebook.exit("extracted 7 rows")
