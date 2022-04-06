from ddselection.data.query_config import *


ent_adj_query = f"""
SELECT
    raw_pred.*
    , ent.l28d_avg_sales
    , ent_sub.l28d_avg_sales AS sub_l28d_avg_sales
    , ent_global.l28d_avg_sales AS global_l28d_avg_sales
    , 0.5 * raw_pred.pred + \
      0.5 * coalesce(
          ent.l28d_avg_sales,
          ent_sub.l28d_avg_sales,
          ent_global.l28d_avg_sales,
          raw_pred.pred
      ) AS pred_ent_adj
FROM
    {{raw_pred_table_name}} raw_pred
LEFT JOIN
    public.fact_selection_intel_mx_raw mx_raw
ON
    raw_pred.match_id = mx_raw.id
LEFT JOIN
    {ent_sp_mx_table_name} ent
ON
    mx_raw.restaurant_name = ent.restaurant_name
    AND mx_raw.sub_id = ent.sub_id
    AND mx_raw.sp_id = ent.sp_id
    AND raw_pred.create_date = ent.created_date
LEFT JOIN
    {ent_sub_mx_table_name} ent_sub
ON
    mx_raw.restaurant_name = ent_sub.restaurant_name
    AND mx_raw.sub_id = ent_sub.sub_id
    AND raw_pred.create_date = ent_sub.created_date
LEFT JOIN
    {ent_global_mx_table_name} ent_global
ON
    mx_raw.restaurant_name = ent_global.restaurant_name
    AND raw_pred.create_date = ent_global.created_date
WHERE
    raw_pred.create_date = '{{test_date}}'
"""
