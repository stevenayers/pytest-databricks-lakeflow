# Databricks notebook source
import json

names = ["count", "row", "rows"]
dbutils.notebook.exit(json.dumps({name: dbutils.widgets.get(name) for name in names}))
