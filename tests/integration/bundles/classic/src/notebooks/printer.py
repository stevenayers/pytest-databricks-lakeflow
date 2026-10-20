# Databricks notebook source
import sys

rows = spark.range(3).count()
print(f"printer: {rows} rows")
print("printer: to stderr", file=sys.stderr)
