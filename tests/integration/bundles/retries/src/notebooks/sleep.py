# Databricks notebook source
import time

# Job parameters arrive as widgets.
seconds = int(dbutils.widgets.get("seconds"))
time.sleep(seconds)
dbutils.notebook.exit(f"slept {seconds} seconds")
