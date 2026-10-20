# Databricks notebook source
import json

seen = {name: dbutils.widgets.get(name) for name in ("kind", "location", "day")}
dbutils.notebook.exit(json.dumps(seen))
