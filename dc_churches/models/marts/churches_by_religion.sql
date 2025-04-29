{{ config(materialized='table') }}

select
    case
        when upper(trim(religion)) in ('BUDDHIST', 'BUDDHISM') then 'BUDDHIST'
        when religion is null or trim(religion) = '' then 'UNKNOWN'
        else upper(trim(religion))
    end as religion,
    count(*) as total_churches
from {{ ref('stg_churches') }}
where religion is not null and trim(religion) <> ''
group by 1
order by total_churches desc
