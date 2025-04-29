{{ config(materialized='table') }}

select
    church_name,
    religion,
    category,
    longitude_local as longitude,
    latitude_local as latitude,
    address,
    raw_web_url
from {{ ref('stg_churches') }}
where longitude_local is not null and latitude_local is not null
