with _data as (select * from {{ ref("silver__sales") }}) select * from _data
