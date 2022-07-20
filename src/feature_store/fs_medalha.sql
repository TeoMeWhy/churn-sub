select '{date}' as dtRef,
       t1.idPlayer,
       count(distinct t1.idMedal) as qtMedalhaDist,
       count(t1.idMedal) as qtMedalha,
       
       count( case when t2.descMedal in ('#YADINHO - Eu Fui!',
                                         'Missão da Tribo',
                                         'Tribo Gaules') then t1.idMedal end ) as qtMedalhaTribo,
                                         
       count( case when t2.descMedal = 'Experiência de Batalha' then t1.idMedal end ) as qtExpBatalha

from bronze_gc.tb_players_medalha as t1

left join bronze_gc.tb_medalha as t2
on t1.idMedal = t2.idMedal

where t1.dtCreatedAt < t1.dtExpiration
and t1.dtCreatedAt < coalesce( t1.dtRemove, now())
and t1.dtCreatedAt < '{date}'

group by t1.idPlayer