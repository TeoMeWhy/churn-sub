-- Databricks notebook source
select dtRef,
       count(*),
       count(distinct idPlayer)

from silver_gc.fs_assinatura

group by 1
order by 1
