with tb_assinatura as (
  select
    t1.*,
    t2.descMedal
  from
    bronze_gc.tb_players_medalha as t1
    left join bronze_gc.tb_medalha as t2 on t1.idMedal = t2.idMedal
  where
    t1.dtCreatedAt < t1.dtExpiration
    and t1.dtCreatedAt < coalesce(t1.dtRemove, t1.dtExpiration, now())
    and t1.dtCreatedAt < '{date}'
    and coalesce(t1.dtRemove, t1.dtExpiration, now()) > '{date}'
    and t2.descMedal in ('Membro Premium', 'Membro Plus')
  order by
    idPlayer
),
tb_assinatura_rn as (
  select
    *,
    row_number() over (
      partition by idPlayer
      order by
        dtExpiration desc
    ) as rn_assinatura
  from
    tb_assinatura
),
tb_assinatura_sumario as (
  select
    *,
    datediff('{date}', dtCreatedAt) as qtDiasAssinatura,
    datediff(dtExpiration, '{date}') as qtDiasExpiracaoAssinatura
  from
    tb_assinatura_rn
  where
    rn_assinatura = 1
  order by
    idPlayer
),
tb_assinatura_hist as (
  select
    t1.idPlayer,
    count(t1.idMedal) as qtAssinatura,
    count(
      case
        when t2.descMedal = 'Membro Premium' then t1.idMedal
      end
    ) as qtPremium,
    count(
      case
        when t2.descMedal = 'Membro Plus' then t1.idMedal
      end
    ) as qtPlus
  from
    bronze_gc.tb_players_medalha as t1
    left join bronze_gc.tb_medalha as t2 on t1.idMedal = t2.idMedal
  where
    t1.dtCreatedAt < t1.dtExpiration
    and t1.dtCreatedAt < coalesce(t1.dtRemove, now())
    and t1.dtCreatedAt < '{date}'
    and t2.descMedal in ('Membro Premium', 'Membro Plus')
  group by
    t1.idPlayer
)
select
  '{date}' as dtRef,
  t1.idPlayer,
  t1.descMedal,
  1 as flAssinatura,
  t1.qtDiasAssinatura,
  t1.qtDiasExpiracaoAssinatura,
  t2.qtAssinatura,
  t2.qtPremium,
  t2.qtPlus
from
  tb_assinatura_sumario as t1
  left join tb_assinatura_hist as t2 on t1.idPlayer = t2.idPlayer