select status, count(*) as orders
from {{ ref("orders") }}
group by status
