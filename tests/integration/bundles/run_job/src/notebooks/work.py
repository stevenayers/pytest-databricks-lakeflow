# Databricks notebook source
dbutils.widgets.text("greeting", "hi")
dbutils.notebook.exit(f"{dbutils.widgets.get('greeting')} from the child")
