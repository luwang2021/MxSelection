def calucate_rr(target_table, params, country_id = '1,2,5', cohort = 'Enterprise\',\'Mid-Market\',\'Local', activation_date='2021-03-31'):
  rr_qry = """
    WITH NATIONAL as (
    SELECT
    distinct l.business_id
    FROM doordash_merchant.public.maindb_businesss_group_link l
    left join doordash_merchant.public.maindb_business_group g
    ON l.business_group_id = g.id
    where g.is_active = TRUE
    AND g.is_test = FALSE
    AND g.business_group_type = 'ENTERPRISE'
  ), MANAGED AS (
    SELECT
    DISTINCT ds.business_id
    FROM proddb.public.dimension_store ds
    left join doordash_merchant.public.maindb_businesss_group_link l on l.business_id = ds.BUSINESS_ID
    left join doordash_merchant.public.maindb_business_group bg on bg.id = l.business_group_id
    where business_group_type = 'MID_MARKET'
       and bg.is_active = TRUE
       and bg.is_test = FALSE
  ), grain as (
      select
         DISTINCT to_date(pred_date) + 1 AS activation_date, --- Activation date
         b.date_stamp::DATE     AS dm_date
      from {tbname} a
      left join static.dm_date b
        on to_date(b.date_stamp)
        between DATEADD('day', -83, to_date(pred_date) + 1)
        AND to_date(pred_date) + 1
      LEFT JOIN fact_selection_intel_mx_raw mx_raw
          on a.match_id = mx_raw.id
      LEFT JOIN fact_region fr
        on mx_raw.sub_id = fr.submarket_id
      WHERE DECILE_RANK_PRED is not null
      order by 1,2
  ), rolling as (
    select
         a.activation_date,
         a.dm_date,
         c.nimda_id as store_id,
         c.sub_id,
         b.match_id,
         b.DECILE_RANK_PRED ,
         b.decile_rank_actual,
         b.TAM_VALUE_PRED AS TAM_VALUE_PRED,
         b.TAM_VALUE_ACTUAL,
    case when p.match_id is not null
         then 1
         ELSE 0 END AS is_poor_act_mx,
         MAX(CASE WHEN n.business_id is not null
            then 'Enterprise'
            WHEN mm.business_id is not null
            then 'Mid-Market'
            ELSE 'Local' END) AS cohort
    from grain a
    left join {tbname} b
      on a.dm_date = to_date(b.pred_date) + 1
      inner join fact_selection_intel_mx_raw c
        on b.match_id = c.id
      inner JOIN dimension_store ds
        on c.nimda_id = ds.store_id
      LEFT JOIN NATIONAL n
      on ds.business_id=n.BUSINESS_ID
      LEFT JOIN MANAGED mm
      on ds.business_id=mm.BUSINESS_ID
      LEFT JOIN fact_selection_intel_poor_activations_mx p
      on b.match_id = p.match_id
    WHERE
      DECILE_RANK_PRED is not null
      AND upper(c.restaurant_name) not like '%JUST WINGS%' -- remove VB
      AND (CASE WHEN 0 = 1
                THEN is_poor_act_mx =0
                ELSE 1=1 END)
    group by 1,2,3,4,5,6,7,8,9,10
    HAVING COHORT IN ('{cohort}')
  ),det as (
    select
     activation_date,
     DECILE_RANK_ACTUAL,
     DECILE_RANK_PRED,
     --b.score,
     count(DISTINCT dm_date) AS rolling_weeks_cnt,
     count(DISTINCT a.match_id)                 AS store_cnt,
     SUM(abs(DECILE_RANK_PRED - DECILE_RANK_ACTUAL))       AS balance_score
     from rolling a
     LEFT JOIN fact_region fr
        on a.sub_id = fr.submarket_id
     WHERE fr.country_id in ({country_id})
  group by 1,2,3
  )
  select
  -- DECILE_RANK_ACTUAL,
  activation_date,
  sum(store_cnt)                           AS tot_store_cnt,
  sum(balance_score)                       AS tot_balance_score,
  round(tot_balance_score/tot_store_cnt,2)          AS normalized_balance_score
  from det
  where activation_date = '{activation_date}'
  group by 1
  order by 1
  """.format(tbname = target_table, country_id = country_id, cohort = cohort, activation_date = activation_date)
  df = load_data(params=params, query=rr_qry)
  return df


def calucate_weighted_cpe(target_table, params, country_id = '1,2,5', cohort = 'Enterprise\',\'Mid-Market\',\'Local', activation_date='2021-03-31'):
  weighted_cpe_qry = """
    WITH NATIONAL as (
    SELECT
    distinct l.business_id
    FROM doordash_merchant.public.maindb_businesss_group_link l
    left join doordash_merchant.public.maindb_business_group g
    ON l.business_group_id = g.id
    where g.is_active = TRUE
    AND g.is_test = FALSE
    AND g.business_group_type = 'ENTERPRISE'
  ), MANAGED AS (
    SELECT
    DISTINCT ds.business_id
    FROM proddb.public.dimension_store ds
    left join doordash_merchant.public.maindb_businesss_group_link l on l.business_id = ds.BUSINESS_ID
    left join doordash_merchant.public.maindb_business_group bg on bg.id = l.business_group_id
    where business_group_type = 'MID_MARKET'
       and bg.is_active = TRUE
       and bg.is_test = FALSE
  ), grain as (
    select
       DISTINCT to_date(pred_date) + 1 AS activation_date, --- Activation date
       b.date_stamp::DATE     AS dm_date
    from {tbname} a
    left join static.dm_date b
      on to_date(b.date_stamp)
      between DATEADD('day', -83, to_date(pred_date) + 1)
      AND to_date(pred_date) + 1
    WHERE DECILE_RANK_PRED is not null
      order by 1,2
  ), rolling as (
    select
         a.activation_date,
         a.dm_date,
         c.nimda_id as store_id,
         c.sub_id,
         b.match_id,
         b.DECILE_RANK_PRED ,
         b.TAM_VALUE_PRED AS TAM_VALUE_PRED,
         b.TAM_VALUE_ACTUAL,
         case when p.match_id is not null
         then 1
         ELSE 0 END AS is_poor_act_mx,
         MAX(CASE WHEN n.business_id is not null
            then 'Enterprise'
            WHEN mm.business_id is not null
            then 'Mid-Market'
            ELSE 'Local' END) AS cohort
    from grain a
    left join {tbname} b
      on a.dm_date = to_date(b.pred_date) + 1
      inner join fact_selection_intel_mx_raw c
        on b.match_id = c.id
      inner JOIN dimension_store ds
        on c.nimda_id = ds.store_id
      LEFT JOIN NATIONAL n
      on ds.business_id=n.BUSINESS_ID
      LEFT JOIN MANAGED mm
      on ds.business_id=mm.BUSINESS_ID
      LEFT JOIN fact_selection_intel_poor_activations_mx p
      on b.match_id = p.match_id
      LEFT JOIN fact_region fr
      on c.sub_id = fr.submarket_id
    WHERE
        DECILE_RANK_PRED is not null
        AND upper(c.restaurant_name) not like '%JUST WINGS%' -- remove VB
        AND (CASE WHEN 0= 1
                THEN is_poor_act_mx =0
                ELSE 1=1 END)
        AND fr.country_id in ({country_id})
    group by 1,2,3,4,5,6,7,8,9
    HAVING COHORT IN ('{cohort}')
  ), decile_rank_weight as (
     select
        activation_date,
        DECILE_RANK_PRED,
        count(DISTINCT store_id)                 AS store_cnt,
        AVG(TAM_VALUE_ACTUAL)                    AS actual_avg,
        sum(actual_avg) over (partition by activation_date) AS actual_avg_tot,
        actual_avg/actual_avg_tot                AS decile_weight
     from rolling r
     group by 1,2
     order by 1,2
  ),calc_cpe as (
    select
     activation_date,
     DECILE_RANK_PRED,
     count(DISTINCT DATE_TRUNC('week',dm_date)) AS rolling_weeks_cnt,
     AVG(TAM_VALUE_ACTUAL)                      AS avg_tam_value_actual,
     AVG(TAM_VALUE_PRED)                        AS avg_tam_value_pred,
     abs((avg_tam_value_actual - avg_tam_value_pred)/nullif(avg_tam_value_actual, 0)) as avg_rolling_cpe
  from rolling
  group by 1,2
  order by 1,2
  )
  select
     a.activation_date,
     --a.rolling_weeks_cnt,
     sum(avg_rolling_cpe*decile_weight)*100 as Weighted_CPE
  from calc_cpe a
  LEFT JOIN decile_rank_weight b
  ON a.activation_date = b.activation_date
  and a.decile_rank_pred = b.decile_rank_pred
  where a.activation_date = '{activation_date}'
  group by 1
  order by 1
  """.format(tbname = target_table, country_id = country_id, cohort = cohort, activation_date = activation_date)
  df = load_data(params=params, query=weighted_cpe_qry)
  return df
