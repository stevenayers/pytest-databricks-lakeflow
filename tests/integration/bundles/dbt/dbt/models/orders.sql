{{ config(tags=["ci"]) }}

select 1 as order_id, 'open' as status
union all
select 2 as order_id, 'shipped' as status
