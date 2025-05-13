with t as (
  select date(datetime) as dt, client_id, currency,
         sum(case when transaction_type='deposit' then amount end) as inflow,
         sum(case when transaction_type='withdrawal' then amount end) as outflow,
         count(*) as tx_cnt
  from {{ ref('stg_transactions') }}
  group by 1,2,3)
select *, inflow-outflow as net_flow from t
