-- Databricks notebook source
select *

from silver_gc.fs_assinatura

order by idPlayer, dtRef
