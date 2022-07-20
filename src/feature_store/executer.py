# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import datetime
from tqdm import tqdm

# COMMAND ----------

dt_start = dbutils.widgets.get("dt_start")
dt_stop = dbutils.widgets.get("dt_stop")

fs_name = dbutils.widgets.get("feature_store")
database = dbutils.widgets.get("database_target")

database_table = f"{database}.{fs_name}"

# COMMAND ----------

def import_query(path):
    with open(path, 'r') as open_file:
        query = open_file.read()
    return query

def table_exists(database, table):
    return (spark.sql(f"show tables from {database}")
                 .filter(f"tableName = '{table}'")
                 .count()) > 0
    
def date_range(start, stop):
    dates = []
    
    dt_start = datetime.datetime.strptime(start, "%Y-%m-%d")
    dt_stop = datetime.datetime.strptime(stop, "%Y-%m-%d")
    while dt_start <= dt_stop:
        dates.append( dt_start.strftime("%Y-%m-%d") )
        dt_start += datetime.timedelta(days=1)
    return dates

def exec_one(query, date):
    query_exec = query.format(date=date)
    df = spark.sql(query_exec)
    return df

def exec(query, dates, database, table):
    
    if not table_exists(database, table):
        date = dates.pop(0)
        df = exec_one(query, date)
        (df.coalesce(1)
           .write
           .mode("overwrite")
           .format("delta")
           .partitionBy("dtRef")
           .saveAsTable(database_table))
        
    for date in tqdm(dates):
        spark.sql(f"DELETE FROM {database}.{table} WHERE dtRef = '{date}' ")

        df = exec_one(query, date)
        (df.coalesce(1)
           .write
           .mode("append")
           .format("delta")
           .saveAsTable(database_table))

# COMMAND ----------

query = import_query(f"{fs_name}.sql")

dates = date_range(dt_start, dt_stop)

exec(query=query, dates=dates, database=database, table=fs_name)
