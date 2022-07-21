-- Databricks notebook source
drop table if exists silver_gc.abt_model_churn;
create table silver_gc.abt_model_churn as

with tb_features as (
  select t1.*,
  
        t3.qtMedalhaDist,
        t3.qtMedalha,
        t3.qtMedalhaTribo,
        t3.qtExpBatalha,
         
        t2.qtPartidas,
        t2.qtDias,
        t2.propDia01,
        t2.propDia02,
        t2.propDia03,
        t2.propDia04,
        t2.propDia05,
        t2.propDia06,
        t2.propDia07,
        t2.qtRecencia,
        t2.winRate,
        t2.avgHsRate,
        t2.vlHsHate,
        t2.avgKDA,
        t2.vlKDA,
        t2.avgKDR,
        t2.vlKDR,
        t2.avgKill,
        t2.avgAssist,
        t2.avgDeath,
        t2.avgHs,
        t2.avgBombeDefuse,
        t2.avgBombePlant,
        t2.avgTk,
        t2.avgTkAssist,
        t2.avg1Kill,
        t2.avg2Kill,
        t2.avg3Kill,
        t2.avg4Kill,
        t2.avg5Kill,
        t2.avgPlusKill,
        t2.avgFirstKill,
        t2.avgDamage,
        t2.avgHits,
        t2.avgShots,
        t2.avgLastAlive,
        t2.avgClutchWon,
        t2.avgRoundsPlayed,
        t2.avgSurvived,
        t2.avgTrade,
        t2.avgFlashAssist,
        t2.propAncient,
        t2.propOverpass,
        t2.propVertigo,
        t2.propNuke,
        t2.propTrain,
        t2.propMirage,
        t2.propDust2,
        t2.propInferno,
        t2.vlLevel

  from silver_gc.fs_assinatura as t1

  left join silver_gc.fs_gameplay as t2
  on t1.dtRef = t2.dtRef
  and t1.idPlayer = t2.idPlayer

  left join silver_gc.fs_medalha as t3
  on t1.dtRef = t3.dtRef
  and t1.idPlayer = t3.idPlayer

  where t1.dtRef <= date_sub('2022-02-10',30)
)

select t1.*,
       coalesce(t2.flAssinatura,0) as flNaoChurn

from tb_features as t1

left join silver_gc.fs_assinatura as t2
on t1.idPlayer = t2.idPlayer
and t1.dtRef = date_sub(t2.dtRef, 30)
