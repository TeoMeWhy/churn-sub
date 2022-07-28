# Databricks notebook source
# MAGIC %pip install feature-engine==1.4.1 scikit-plot

# COMMAND ----------

import pandas as pd

import scikitplot as skplt

from sklearn import model_selection
from sklearn import pipeline
from sklearn import tree
from sklearn import ensemble
from sklearn import metrics

from feature_engine import imputation
from feature_engine import encoding

# COMMAND ----------

# DBTITLE 1,Sample
df = spark.table("silver_gc.abt_model_churn").toPandas()

columns = df.columns

target = 'flNaoChurn'
ids = ['dtRef', 'idPlayer']
to_remove = ['flAssinatura']

features = list(set(columns) - set([target]) - set(ids) - set(to_remove))

X_train, X_test, y_train, y_test = model_selection.train_test_split(df[features],
                                                                    df[target],
                                                                    test_size=0.2,
                                                                    random_state=42)

print("Taxa de resposta treino:", 100*y_train.mean().round(4), "%")
print("Taxa de resposta teste:", 100*y_test.mean().round(4), "%")

# COMMAND ----------

# DBTITLE 1,Explore
# Identificando missings

missing_columns = X_train.count()[X_train.count() < X_train.shape[0]].index.tolist()

missing_columns.sort()
missing_columns

missings_flag = [
    'avg1Kill',
    'avg2Kill',
    'avg3Kill',
    'avg4Kill',
    'avg5Kill',
    'avgAssist',
    'avgBombeDefuse',
    'avgBombePlant',
    'avgClutchWon',
    'avgDamage',
    'avgDeath',
    'avgFirstKill',
    'avgFlashAssist',
    'avgHits',
    'avgHs',
    'avgHsRate',
    'avgKDA',
    'avgKDR',
    'avgKill',
    'avgLastAlive',
    'avgPlusKill',
    'avgRoundsPlayed',
    'avgShots',
    'avgSurvived',
    'avgTk',
    'avgTkAssist',
    'avgTrade',
    'qtRecencia',
    'vlHsHate',
    'vlKDA',
    'vlKDR',
    'vlLevel',
    'winRate']

missing_zero = [
    'propAncient',
    'propDia01',
    'propDia02',
    'propDia03',
    'propDia04',
    'propDia05',
    'propDia06',
    'propDia07',
    'propDust2',
    'propInferno',
    'propMirage',
    'propNuke',
    'propOverpass',
    'propTrain',
    'propVertigo',
    'qtDias',
    'qtPartidas',
]

cat_features = X_train.dtypes[X_train.dtypes == 'object'].index.tolist()

# COMMAND ----------

X_train.describe()

# COMMAND ----------

# DBTITLE 1,Modify
fe_missing_flag = imputation.ArbitraryNumberImputer(arbitrary_number=-100,
                                                    variables=missings_flag)

fe_missing_zero = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                                    variables=missing_zero)

fe_onehot = encoding.OneHotEncoder(variables=cat_features)

# COMMAND ----------

# DBTITLE 1,Modeling
model = ensemble.RandomForestClassifier(min_samples_leaf=25, n_estimators=250)

model_pipeline = pipeline.Pipeline( [ ("Missing Flag", fe_missing_flag),
                                      ("Missing Zero", fe_missing_zero),
                                      ("OneHot", fe_onehot),                                     
                                      ("Classificador", model),
                                    ] )

model_pipeline.fit(X_train, y_train)

# COMMAND ----------

y_train_predict = model_pipeline.predict(X_train)

acc_train = metrics.accuracy_score(y_train, y_train_predict)
print("Acurácia treino:", acc_train)

# COMMAND ----------

y_test_predict = model_pipeline.predict(X_test)
y_probas = model_pipeline.predict_proba(X_test)

acc_test = metrics.accuracy_score(y_test, y_test_predict)
print("Acurácia teste:", acc_test)

# COMMAND ----------

features_fit = model_pipeline[:-1].transform(X_train).columns.tolist()

features_importance = pd.Series(model.feature_importances_, index=features_fit)
features_importance.sort_values(ascending=False).head(15)

# COMMAND ----------

skplt.metrics.plot_roc(y_test, y_probas)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_test, y_probas)

# COMMAND ----------

skplt.metrics.plot_cumulative_gain(y_test, y_probas)

# COMMAND ----------

skplt.metrics.plot_lift_curve(y_test, y_probas)

# COMMAND ----------

2.1*(1 - y_test.mean()) # taxa de churn 

# COMMAND ----------



# COMMAND ----------

df_probas = pd.DataFrame(
    {"probas": y_probas[:,0],
     "target": y_test
    }
)

df_probas = df_probas.sort_values(by="probas", ascending=False)
df_probas
1-df_probas.head(int(df_probas.shape[0] * 0.2))["target"].mean()
