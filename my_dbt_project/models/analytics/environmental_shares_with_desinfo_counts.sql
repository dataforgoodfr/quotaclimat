{{ config(
    materialized='incremental',
    unique_key=['start','channel_name','country']
  )
}}

with env_shares as (
	with name_map as (
		select 
			channel_title,
			max(channel_name) channel_name
		from 
			program_metadata pm
		where pm.country='france'
		group by
			channel_title
	)
	 select
	 	start,
		cqes."Program Metadata - Channel Name__channel_title" as "channel_title",
		name_map.channel_name,
		cqes.sum_duration_minutes,
		cqes."% climat" as weekly_perc_climat,
		'france' as country
	from 
		public.core_query_environmental_shares cqes
	left join
		name_map 
	on
		name_map.channel_title=cqes."Program Metadata - Channel Name__channel_title"
	union all
	select 
		cqesin."start",
		cqesin.channel_title,
		cqesin.channel_name,
		cqesin.sum_duration_minutes,
		cqesin."% climat" as weekly_perc_climat,
		country
	from 
		public.core_query_environmental_shares_i8n cqesin
	where country!='france'
),
weekly_desinfo as (
	select 
	 	date_trunc('week', tgc.data_item_start) week_start,
	 	tgc.data_item_channel_name,
	 	tgc.country,
	 	sum(case when tgc.mesinfo_correct is null then 0 else tgc.mesinfo_correct end) total_mesinfo
	from
		{{ ref("task_global_completion") }} tgc
    where tgc."Annotation Version"=1
	group by
		week_start,
	 	tgc.data_item_channel_name,
	 	tgc.country
)
select 
	env_shares.*,
	case when weekly_desinfo.total_mesinfo is null then 0 else weekly_desinfo.total_mesinfo end total_mesinfo
from 
	env_shares
left join 
	weekly_desinfo
on
	env_shares.start=weekly_desinfo.week_start
	and env_shares.channel_name=weekly_desinfo.data_item_channel_name
	and env_shares.country=weekly_desinfo.country
