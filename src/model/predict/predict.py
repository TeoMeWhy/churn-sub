# Databricks notebook source
# MAGIC %pip install feature-engine==1.4.1

# COMMAND ----------

import datetime

import mlflow

from delta.tables import * 


def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

# COMMAND ----------

# ETL
query = import_query("etl.sql")
df = spark.sql(query).toPandas()

# Import do modelo
model = mlflow.sklearn.load_model("models:/churn-sub-gc/production")

# Predict do modelo
churn_score = model.predict_proba(df[model.feature_names_in_])[:,0]

# COMMAND ----------

df['churn_score'] = churn_score
df['dtIngestion'] = datetime.datetime.now()

sdf = spark.createDataFrame(df[['dtRef','idPlayer','churn_score', 'dtIngestion']])
sdf.display()

# COMMAND ----------

table_exists = (spark.sql("show tables in silver_gc")
                     .filter("tableName = 'model_churn'").count()) > 0

if not table_exists:
    sdf.coalesce(1).write.format("delta").partitionBy("dtRef").saveAsTable("silver_gc.model_churn")

else:
    table = DeltaTable.forName(spark,'silver_gc.model_churn')
    
    (table.alias("t")
          .merge(sdf.alias("s"),"t.idPlayer = s.idPlayer and t.dtRef = s.dtRef")
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute() )
