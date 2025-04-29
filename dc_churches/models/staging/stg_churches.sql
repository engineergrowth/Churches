{{ config(materialized='view') }}

with deduplicated as (

    select
        objectid,
        upper(trim(name)) as church_name,
        case
            when web_url is null or trim(web_url) = '' or lower(trim(web_url)) = '<null>' or lower(trim(web_url)) = 'null'
            then null
            else trim(web_url)
        end as raw_web_url,
        upper(trim(religion)) as religion,
        upper(trim(place_of_worship)) as category,
        x as longitude_raw,
        y as latitude_raw,
        xcoord as longitude_local,
        ycoord as latitude_local,
        upper(trim(address)) as address,

        row_number() over (
            partition by lower(trim(name)), lower(trim(address))
            order by objectid desc  
        ) as row_num

    from {{ source('raw_data', 'CHURCHES_RAW') }}
    where is_place_of_worship = 1

)

select
    objectid,
    church_name,
    raw_web_url,
    religion,
    category,
    longitude_raw,
    latitude_raw,
    longitude_local,
    latitude_local,
    address
from deduplicated
where row_num = 1
