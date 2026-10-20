# Databricks notebook source
import json

# No task values: an iteration of a for-each task cannot set them.
dbutils.notebook.exit(json.dumps({name: dbutils.widgets.get(name) for name in ("item", "item_id", "rows")}))
