import logging
import pandas as pd

from ddselection.data.utils import load_data

logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def tam_calculation_v1(
    prediction_table_name,
    schema,
    params,
    pred_col_name,
    pred_date,
    testdate='2019-11-28'
):
    """Calculate tam values based on model predictions, query execution

    Args:
        prediction_table_name (str): the table name of predictions
        schema                (str): snowflake schema
        params                (dict): params to access snowflake
        pred_col_name         (str): the column name of the prediction
        pred_date             (str): prediction date
        testdate              (str): the date of testing or current date
    Returns:
        pandas DF: calculated tam values for all mx
    """
    query = """
        --Active in last 7 days?
        WITH mx_active_base as (
            select
                a.date_stamp,
                a.store_id,
                a.business_id,
                a.sp_id,
                a.sp_name,
                a.sub_id,
                a.sub_name,
                case when (partner_eop = 1 and active_eop = 1) or (partner_bop = 1 and active_bop = 1) then 1 else 0 end as active_partner,
                case when (active_eop = 1 or active_bop = 1) then 1 else 0 end as active,
                sum(active_partner) over (partition by a.store_id order by a.DATE_STAMP asc ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as last_7_active_partner,
                sum(active) over (partition by a.store_id order by a.DATE_STAMP asc ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as last_7_active
            from public.fact_store_active_status a
            where
                date_stamp > dateadd('days', -20, '{testdate}')
                AND date_stamp < '{testdate}'
            order by 2,1
        )
        --Flag if active or active partner in last 7 days
        , mx_active_base_cleaned as (
            select
                date_stamp,
                store_id,
                business_id,
                sp_id,
                sp_name,
                sub_id,
                sub_name,
                case when last_7_active_partner > 0 then 1 else 0 end as active_partner,
                case when last_7_active > 0 then 1 else 0 end as active
            from
                mx_active_base
            where
                date_stamp > dateadd('days', -20, '{testdate}')
                AND date_stamp < '{testdate}'
        )
        --Live Partners
        , mx_active_partners as (
            select
                *
            from
                mx_active_base_cleaned
            where
                date_stamp = (select max(date_stamp) from mx_active_base_cleaned)
                and active_partner = 1 --pick max date
        )

        --Live NonPartners
        , mx_active_nps as (
            select
                *
            from
                mx_active_base_cleaned
            where
                date_stamp = (select max(date_stamp) from mx_active_base_cleaned)
                and active = 1
                and active_partner = 0 --pick max date
        )

        --Live Partner Deatils from Mx DB
        , mx_live_merchants AS (
            select
                  mx.id as match_id
                , st.store_id
                , mx.vendor_1_id
                , mx.vendor_2_id
                , mx.restaurant_name as restaurant_name
                , st.sub_id as submarket_id
                , st.sp_id as starting_point_id
                , mx.national
                , st.active
                , st.active_partner
                , 'live sales' as model_id
            from
                mx_active_partners st
            left join
                public.fact_selection_intel_mx_raw mx ON mx.nimda_id = st.store_id
        )

        --Last 28 day live sales
        , mx_last_twenty_eight_day_perf AS (
            SELECT
                DD.STORE_ID,
                SUM(DD.SUBTOTAL / 100.0) AS TAM_VALUE
            FROM
                PUBLIC.DIMENSION_DELIVERIES AS DD
            LEFT JOIN
                public.dimension_store st ON st.store_id = dd.store_id
            WHERE
                DD.CREATED_AT BETWEEN dateadd('DAY', -29, '{testdate}') AND dateadd('DAY', -1, '{testdate}')
                AND DD.is_filtered_core = 1
                AND dd.is_partner = 1
            GROUP BY
                1
        )

        --Pull in last 28 day sales for live partners
        , mx_partner_sales AS (
            select
                lm.match_id
                ,lm.store_id
                ,lm.vendor_1_id
                ,lm.vendor_2_id
                ,lm.restaurant_name
                ,lm.national
                ,lm.submarket_id
                ,lm.starting_point_id
                ,lm.active
                ,lm.active_partner
                ,IFNULL(lp.TAM_VALUE, 0) as TAM_VALUE
                ,lm.model_id
                ,dateadd('DAY', -1, '{testdate}') as dte
            from
                mx_live_merchants lm
            left join
                mx_last_twenty_eight_day_perf lp
            ON
                lp.store_id = lm.store_id
        )

        /********************************
        ON PLATFORM [end]
        ********************************/


        /********************************
        LEAD VALIDATION for Off Platform [start]
        ********************************/

        , mx_mx_google_combined_raw AS (
            SELECT
                m.*
                ,(CASE when vendor_1_id is not null then 1 else 0 end) + (CASE when vendor_2_id is not null then 1 else 0 end)
                 + (CASE when nimda_id is not null then 1 else 0 end) + (CASE when factual_id is not null then 1 else 0 end)
                 + (CASE when chd_id is not null then 1 else 0 end) as number_sources

                --clean up case where no record found in google
                ,case when g.google_place_id is null then null else g.is_valid_restaurant end as is_valid_restaurant,
                case when g.google_place_id is null then null else g.PERMANENTLY_CLOSED end as PERMANENTLY_CLOSED

            FROM
                proddb.PUBLIC.FACT_SELECTION_INTEL_MX_RAW m
            left join
                --proddb.STATIC.lead_validation_predictions_2021q4 g
                proddb.STATIC.FACT_SELECTION_INTEL_MX_VERIFICATION_2020_Q1 g
            ON
                g.mx_id = m.id
         )

        , mx_valid_records AS (
            select
                distinct m.id
            from
                mx_mx_google_combined_raw m
            left join
                public.dimension_store s
            on
                s.store_id = m.nimda_id
            WHERE
                -- hack, add stores activated after th prediction date
                s.activated_at > '{pred_date}'
                OR
            -- keep everything nimda active
               ((s.is_active = 1 and s.is_partner = 0)

            --if above factual threshold filter google closed
                OR (((factual_existence_score >=0.6 and country='USA')
                        or (factual_existence_score >=0.6 and country<>'USA'))
                        and (ifnull(m.permanently_closed,0)=0))
            -- else use google closed + is_restaurant
                OR (((factual_existence_score <0.6 and country='USA')
                        or (factual_existence_score <0.6 and country<>'USA')
                        or factual_id is null)
                        and
                        ((is_valid_restaurant is null or is_valid_restaurant = true) and ifnull(m.permanently_closed,0) =0)
                    )

            --cut all factual below .3
                AND ((factual_existence_score > 0 and country='USA')
                        or (factual_existence_score > 0 and country<>'USA')
                        or factual_id is null)

            --consensus rule if not in google
                AND ( ((is_valid_restaurant = 1) and ifnull(m.permanently_closed,0) =0)
                        or (number_sources > 1)
                        or (factual_existence_score > .5) )


             --and remove if there's bad data
                AND (m.RESTAURANT_NAME is not NULL
                    AND m.STREET is not NULL
                    AND m.lat is not NULL
                    AND m.lng is not null
                    AND m.sp_id is not null))
        )
        , mx_valid_r AS (
            select
                p.*
            from
                {schema}.{table_name} p
            join
                mx_valid_records vr
            on
                p.match_id = vr.id
            where
                create_date = '{pred_date}'
        )

        /********************************
        LEAD VALIDATION for Off Platform [end]
        ********************************/


        /********************************
        Off Platform & NP Predictions [start]
        ********************************/


        , mx_np_preds_table AS (
            select
                  p.match_id as match_id
                , p.store_id
                , p.vendor_1_id
                , p.vendor_2_id
                , coalesce(p.external_store_name, st.name) as merchant_name
                , mx.national
                , p.submarket_id
                , p.starting_point_id
                , ifnull(zz.active,0) as active
                , ifnull(zz.active_partner, 0) as active_partner
                , CASE when p.{pred_col_name} < 0 then 0 else p.{pred_col_name} end as tam_value
                , p.model_id
                , dateadd('DAY', -1, '{testdate}') as dte
                , ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY tam_value desc nulls last) as dedupe
            from
                mx_valid_r p
            left join
                public.dimension_store st
            ON
                st.store_id = p.store_id
            left join
                doordash_merchant.public.maindb_external_store es
            ON
                es.external_id = p.vendor_1_id
            left join
                (select * from mx_active_base_cleaned where date_stamp = (select max(date_stamp) from public.fact_store_active_status)) zz
            on
                p.store_id = zz.store_id
            left join
                public.fact_selection_intel_mx_raw mx
            ON
                mx.id = p.match_id
            where --(active = 1 and active_partner = 0)
                ((st.order_protocol <> 'DRIVE') or (st.order_protocol is null))
                AND ( (st.creation_method <> 'DRIVE_AUTO_ONBOARDING') or (st.creation_method is null) )
            --  AND ((st.is_active = 0) or (st.is_partner = 0) or (st.store_id is null))
                AND ((es.cuisine_type not in ('Scenic Lookout', 'Grocery Store', 'Food Court', 'Gay Bar', 'Butcher', 'Liquor Store',
                                      'Indie Movie Theater', 'Convenience Store', 'Rental Car Location', 'Pedestrian Plaza',
                                     'Nail Salon', 'Office', 'Office', 'Snack Place', 'Park', 'Gym / Fitness Center',
                                     'Building', 'Big Box Store', 'Tech Startup', 'Shopping Mall', 'Bar', 'Chocolate Shop', 'Event Space')) or (es.cuisine_type is null))
                AND merchant_name NOT LIKE '%TEST%'
                AND merchant_name NOT LIKE '%BUFFET%'
                AND merchant_name NOT LIKE '%CATERING%'
                AND merchant_name NOT LIKE '%MARRIOT%'
                AND merchant_name NOT LIKE '%HILTON%'
                AND merchant_name NOT LIKE '%EMBASSY SUITE%'
            -- and ifnull(active_partner, 0) = 0)
         )

        /********************************
        Off Platform & NP Predictions [start]
        ********************************/


        --Combine On & Off Platform Predictions
        select
              ps.match_id
            , ps.store_id
            , ps.vendor_1_id
            , ps.vendor_2_id
            , ps.restaurant_name AS merchant_name
            , ps.national
            , ps.submarket_id
            , ps.starting_point_id
            , ps.active as is_active
            , ps.active_partner as is_partner
            , ps.tam_value
            , ps.model_id
            , ps.dte
        from
            mx_partner_sales ps
        LEFT JOIN
            mx_np_preds_table np
        ON
            ps.match_id = np.match_id
        WHERE
            np.match_id is NULL

        UNION ALL

        select
              match_id
            , store_id
            , vendor_1_id
            , vendor_2_id
            , merchant_name
            , national
            , submarket_id
            , starting_point_id
            , active as is_active
            , active_partner as is_partner
            , tam_value
            , model_id
            , dte
        from
            mx_np_preds_table np
        where
            dedupe = 1;

    """
    df = load_data(
        params=params,
        query=query.format(
            schema=schema,
            table_name=prediction_table_name,
            pred_col_name=pred_col_name,
            pred_date=pred_date,
            testdate=testdate
        ))
    return df


def decile_rank(
    tam_table_name,
    schema,
    params,
    target_col_name,
    partitionby="dte, submarket_id",
    include_partner=True
):
    """Calculate the decile rank

    Args:
        tam_table_name   (str): the table name of calculated tam
        schema           (str): snowflake schema
        params           (dict): params to access snowflake
        target_col_name  (str): the column name of true values
        partitionby      (str): columns to partition
                                default "dte, submarket_id",
        include_partner  (str): whether to include partners in the ranking

    Returns:
        pandas DF: mx with ranks

    """
    query = """
        SELECT
            DISTINCT a.match_id,
            store_id,
            submarket_id,
            {target_col_name},
            ntile(10) over (partition by {partitionby} order by {target_col_name} DESC) as decile_rank
        FROM
            {schema}.{tam_table_name} a
    """
    if not include_partner:
        query = """
            SELECT
                DISTINCT a.match_id,
                store_id,
                submarket_id,
                {target_col_name},
                ntile(10) over (partition by {partitionby} order by {target_col_name} DESC) as decile_rank
            FROM
                {schema}.{tam_table_name} a
            WHERE
                model_id <> 'live sales'
        """
    df = load_data(
        params=params,
        query=query.format(
            tam_table_name=tam_table_name,
            schema=schema,
            target_col_name=target_col_name,
            partitionby=partitionby
        ))
    return df


def combine_actual_pred_rank_df(
    schema,
    params,
    actual_df,
    pred_df,
    raw_prediction_table_name,
    suffixe=('_actual', '_pred'),
    passthrough_cols_from_raw='match_id, create_date as pred_date, model_id, data_preprocess_model_version, ml_model_version, bias_adj_model_version'
):
    """Function to combine predicted ranks with actual ranks

    Args:
        schema                    (str): snowflake schema
        params                    (dict): params to access snowflake
        actual_df                 (pandas DF): df with actual ranks
        pred_df                   (pandas DF): df with actual ranks
        raw_prediction_table_name (str): the table name of predictions
        suffixe                   (tuple): default ('_actual', '_pred'),
        passthrough_cols_from_raw (str): passthrough columns

    Returns:
        pandas DF: combined df for final metrics computation

    """
    tam_data_for_metrics = pd.merge(
        actual_df, pred_df,
        on=['match_id', 'submarket_id'],
        suffixes=suffixe
    )
    # load raw predictions
    query = """
        select
            {passthrough_cols_from_raw}
        from
            {schema}.{table_name}
        where
            activation_date_actual is not NULL
    """.format(
        passthrough_cols_from_raw=passthrough_cols_from_raw,
        schema=schema,
        table_name=raw_prediction_table_name
    )
    data_prediction_raw = load_data(
        params=params,
        query=query
    )
    data_for_metrics = pd.merge(
        data_prediction_raw,
        tam_data_for_metrics,
        on=['match_id'], how='inner'
    )
    return data_for_metrics
