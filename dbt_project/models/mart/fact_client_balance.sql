select dt as date, client_id, currency,
       sum(net_flow) over (partition by client_id,currency
                           order by dt
                           rows between unbounded preceding and 1 preceding) as opening_balance,
       sum(net_flow) over (partition by client_id,currency
                           order by dt) as closing_balance,
       inflow,outflow,tx_cnt as transaction_count
from {{ ref('int_daily_transactions') }}
