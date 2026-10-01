# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Weekday False Handler Task
# MAGIC Executes when the workflow condition evaluates to False (i.e., today is not scheduled for full master table transformation).

# COMMAND ----------
# 1. Retrieve the task value emitted by the parent "WeekdayLookup" task
var = dbutils.jobs.taskValues.get(taskKey="WeekdayLookup", key="weekoutput")

# COMMAND ----------
# 2. Print and log the received weekday value for audit/debugging
print(var)