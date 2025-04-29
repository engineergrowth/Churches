{{ config(materialized='table') }}

select
    case 
        when raw_web_url is not null then 'Has Website'
        else 'No Website'
    end as website_status,
    count(*) as total_churches
from {{ ref('stg_churches') }}
where religion = 'CHRISTIAN'
group by website_status
order by website_status desc
