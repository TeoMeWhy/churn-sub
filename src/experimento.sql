-- Databricks notebook source
-- DBTITLE 1,Tabela de players
select * from bronze_gc.tb_players

-- COMMAND ----------

-- DBTITLE 1,Tabela de estatísticas das partidas
select * from bronze_gc.tb_lobby_stats_player

-- COMMAND ----------

select * from bronze_gc.tb_players_medalha

where dtCreatedAt < dtExpiration
and dtCreatedAt < dtRemove

-- COMMAND ----------

select * from bronze_gc.tb_medalha

-- COMMAND ----------

select max(dtCreatedAt),
       min(dtCreatedAt),
       max(dtExpiration),
       min(dtExpiration),
       max(dtRemove),
       min(dtRemove)


from bronze_gc.tb_players_medalha

-- COMMAND ----------

-- Número de assinantes ativos em determinado dia

select count(distinct idPlayer)

from bronze_gc.tb_players_medalha

where dtCreatedAt < '2022-02-20'
and dtRemove > '2022-02-20'
and dtCreatedAt < dtRemove
and dtCreatedAt < dtExpiration
and idMedal in (1,3)

