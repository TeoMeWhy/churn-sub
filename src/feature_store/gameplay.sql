-- Databricks notebook source
with tb_level as (

  select idPlayer,
         vlLevel,
         row_number() over (partition by idPlayer order by dtCreatedAt desc) as rnPlayer

  from bronze_gc.tb_lobby_stats_player

  where dtCreatedAt < '2022-01-01'
  and dtCreatedAt >= date_add('2022-01-01',-30)

  order by idPlayer, dtCreatedAt
),

tb_level_final as (
  select *
  from tb_level
  where rnPlayer = 1
),

tb_gameplay_stats as (

  select idPlayer,

         count(distinct idLobbyGame) as qtPartidas,
         count(distinct date(dtCreatedAt)) as qtDias,

         count( distinct case when dayofweek(dtCreatedAt) = 1 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia01,
         count( distinct case when dayofweek(dtCreatedAt) = 2 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia02,
         count( distinct case when dayofweek(dtCreatedAt) = 3 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia03,
         count( distinct case when dayofweek(dtCreatedAt) = 4 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia04,
         count( distinct case when dayofweek(dtCreatedAt) = 5 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia05,
         count( distinct case when dayofweek(dtCreatedAt) = 6 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia06,
         count( distinct case when dayofweek(dtCreatedAt) = 7 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia07,

         min(datediff('2022-01-01', dtCreatedAt)) as qtRecencia,

         avg(flWinner) as winRate,

         avg(qtHs / qtKill) as avgHsRate,
         sum(qtHs) / sum(qtKill) as vlHsHate,

         avg((qtKill + qtAssist) / coalesce(qtDeath,1)) as avgKDA,
         coalesce(sum(qtKill + qtAssist)/sum(coalesce(qtDeath,1)),0) as vlKDA,

         avg( coalesce(qtKill,0) / coalesce(qtDeath,1)) as avgKDR,
         sum(coalesce(qtKill,0)) / sum(coalesce(qtDeath,1)) as vlKDR,

        avg(qtKill) as avgKill,
        avg(qtAssist) as avgAssist,
        avg(qtDeath) as avgDeath,
        avg(qtHs) as avgHs,
        avg(qtBombeDefuse) as avgBombeDefuse,
        avg(qtBombePlant) as avgBombePlant,
        avg(qtTk) as avgTk,
        avg(qtTkAssist) as avgTkAssist,
        avg(qt1Kill) as avg1Kill,
        avg(qt2Kill) as avg2Kill,
        avg(qt3Kill) as avg3Kill,
        avg(qt4Kill) as avg4Kill,
        avg(qt5Kill) as avg5Kill,
        avg(qtPlusKill) as avgPlusKill,
        avg(qtFirstKill) as avgFirstKill,
        avg(vlDamage) as avgDamage,
        avg(qtHits) as avgHits,
        avg(qtShots) as avgShots,
        avg(qtLastAlive) as avgLastAlive,
        avg(qtClutchWon) as avgClutchWon,
        avg(qtRoundsPlayed) as avgRoundsPlayed,
        avg(qtSurvived) as avgSurvived,
        avg(qtTrade) as avgTrade,
        avg(qtFlashAssist) as avgFlashAssist,

        count(distinct case when descMapName = 'de_ancient' then idLobbyGame end) / count(distinct idLobbyGame) as propAncient,
        count(distinct case when descMapName = 'de_overpass' then idLobbyGame end) / count(distinct idLobbyGame) as propOverpass,
        count(distinct case when descMapName = 'de_vertigo' then idLobbyGame end) / count(distinct idLobbyGame) as propVertigo,
        count(distinct case when descMapName = 'de_nuke' then idLobbyGame end) / count(distinct idLobbyGame) as propNuke,
        count(distinct case when descMapName = 'de_train' then idLobbyGame end) / count(distinct idLobbyGame) as propTrain,
        count(distinct case when descMapName = 'de_mirage' then idLobbyGame end) / count(distinct idLobbyGame) as propMirage,
        count(distinct case when descMapName = 'de_dust2' then idLobbyGame end) / count(distinct idLobbyGame) as propDust2,
        count(distinct case when descMapName = 'de_inferno' then idLobbyGame end) / count(distinct idLobbyGame) as propInferno

  from bronze_gc.tb_lobby_stats_player

  where dtCreatedAt < '2022-01-01'
  and dtCreatedAt >= date_add('2022-01-01',-30)

  group by idPlayer
)

select '2022-01-01' as dtRef,
       t1.*,
       t2.vlLevel

from tb_gameplay_stats as t1

left join tb_level_final as t2
on t1.idPlayer = t2.idPlayer
