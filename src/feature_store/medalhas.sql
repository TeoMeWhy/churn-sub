-- Databricks notebook source
select t1.idPlayer,
       count(distinct t1.idMedal) as qtMedalhaDist,
       count(t1.idMedal) as qtMedalha,
       
       count( case when t2.descMedal in ('#YADINHO - Eu Fui!',
                                         'Missão da Tribo',
                                         'Tribo Gaules') then t1.idMedal end ) as qtMedalhaTribo,
                                         
       count( case when t2.descMedal = 'Experiência de Batalha' then t1.idMedal end ) as qtExpBatalha,
       
       count( case when t2.descMedal in ('Membro Premium', 'Membro Plus') then t1.idMedal end ) as qtAssinatura,
       count( case when t2.descMedal = 'Membro Premium' then t1.idMedal end ) as qtPremium,
       count( case when t2.descMedal = 'Membro Plus' then t1.idMedal end ) as qtPlus,
       
       max( case when t2.descMedal in ('Membro Premium', 'Membro Plus')
                        and coalesce( t1.dtRemove, now()) > '2022-01-01'       
                 then 1 else 0 end ) as flAssinante

from bronze_gc.tb_players_medalha as t1

left join bronze_gc.tb_medalha as t2
on t1.idMedal = t2.idMedal

where t1.dtCreatedAt < t1.dtExpiration
and t1.dtCreatedAt < coalesce( t1.dtRemove, now())
and t1.dtCreatedAt < '2022-01-01'

group by t1.idPlayer
