{{ 
    config(
        materialized='table',
    )
}}

with unnested AS (
	SELECT
		lta.task_completion_aggregate_id,
		lta.task_aggregate_id,
		lta.created_at,
		elem ->> 'from_name'         AS from_name,
		elem -> 'value' -> 'choices' AS choices_arr,
		CASE jsonb_typeof(elem -> 'value' -> 'text')
			WHEN 'array'  THEN elem -> 'value' -> 'text'
			WHEN 'string' THEN jsonb_build_array(elem -> 'value' -> 'text')
		END                          AS text_arr	
	FROM public.labelstudio_task_completion_aggregate lta
	CROSS JOIN LATERAL jsonb_array_elements(lta.result::jsonb) AS elem
),
-- Expand inner choices/text arrays into scalar rows
flattened AS (
	SELECT task_completion_aggregate_id, task_aggregate_id, created_at, from_name, v AS val
	FROM   unnested, jsonb_array_elements_text(choices_arr::jsonb) v
	WHERE  choices_arr IS NOT NULL
	UNION ALL
	SELECT task_completion_aggregate_id, task_aggregate_id, created_at, from_name, v AS val
	FROM   unnested, jsonb_array_elements_text(text_arr::jsonb) v
	WHERE  text_arr IS NOT NULL
),
choice_annotations AS (
	SELECT
		task_completion_aggregate_id,
		task_aggregate_id,
		created_at,
		string_agg(
			CASE WHEN val IN ('Correct', 'Incorrect') THEN val END,
			',' ORDER BY val
		) AS mesinfo_choice,
		string_agg(
			CASE WHEN val IN ('Journalist', 'Commentator', 'Guest', 'Politician', 'Audience', 'Unknown') THEN val END,
			',' ORDER BY val
		) AS locuteur_choice,
		CASE WHEN SUM(CASE WHEN val = 'Correct'     THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS mesinfo_correct,
		CASE WHEN SUM(CASE WHEN val = 'Incorrect'   THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS mesinfo_incorrect,
		CASE WHEN SUM(CASE WHEN val = 'Journalist'  THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_journalist,
		CASE WHEN SUM(CASE WHEN val = 'Commentator' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_commentator,
		CASE WHEN SUM(CASE WHEN val = 'Guest'       THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_guest,
		CASE WHEN SUM(CASE WHEN val = 'Politician'  THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_politician,
		CASE WHEN SUM(CASE WHEN val = 'Audience'    THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_audience,
		CASE WHEN SUM(CASE WHEN val = 'Unknown'     THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_unknown,
		CASE WHEN SUM(
			CASE WHEN from_name = 'Was Misinformation Corrected?' AND val = 'corrected' THEN 1 ELSE 0 END
		) > 0 THEN 1 ELSE 0 END AS mesinfo_corrected_bool,
		string_agg(
			DISTINCT CASE WHEN from_name = 'Was Misinformation Corrected?' THEN val END,
			',' ORDER BY CASE WHEN from_name = 'Was Misinformation Corrected?' THEN val END
		) AS mesinfo_corrected,
		string_agg(DISTINCT CASE WHEN from_name = 'references'  THEN val END, ',') AS debunk_references,
		string_agg(DISTINCT CASE WHEN from_name = 'claim'       THEN val END, '.') AS claims,
		string_agg(DISTINCT CASE WHEN from_name = 'explanation' THEN val END, '.') AS explanations,
		string_agg(DISTINCT CASE WHEN from_name = 'comments'    THEN val END, '.') AS other_comments
	FROM flattened
	GROUP BY task_completion_aggregate_id, task_aggregate_id, created_at
),
versioned_choices AS (
	SELECT
        ROW_NUMBER() OVER (PARTITION BY choice_annotations.task_aggregate_id ORDER BY choice_annotations.created_at) AS annotation_version,
		choice_annotations.*
	FROM 
		choice_annotations 
),
tgc as (
	SELECT 
		*
	FROM (
		select
			versioned_choices.task_completion_aggregate_id as task_completion_aggregate_id,
			labelstudio_task_aggregate.task_aggregate_id AS task_aggregate_id,
			labelstudio_task_aggregate.created_at AS created_at,
			labelstudio_task_aggregate.updated_at AS updated_at,
			labelstudio_task_aggregate.is_labeled AS is_labeled,
			labelstudio_task_aggregate.project_id AS project_id,
			labelstudio_task_aggregate.country AS country,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'id'])::TEXT AS data_item_id,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'channel'])::TEXT AS data_item_channel,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'channel_name'])::TEXT AS data_item_channel_name,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'channel_title'])::TEXT AS data_item_channel_title,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'channel_program'])::TEXT AS data_item_channel_program,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'channel_program_type'])::TEXT AS data_item_channel_program_type,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'day'])::DECIMAL AS data_item_day,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'month'])::DECIMAL AS data_item_month,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'year'])::DECIMAL AS data_item_year,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'start'])::TIMESTAMP AS data_item_start,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'model_name'])::TEXT AS data_item_model_name,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'model_reason'])::TEXT AS data_item_model_reason,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'model_result'])::DECIMAL AS data_item_model_result,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'plaintext'])::TEXT AS data_item_plaintext,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'plaintext_whisper'])::TEXT AS data_item_plaintext_whisper,
			(labelstudio_task_aggregate.data::jsonb #>> ARRAY['item', 'url_mediatree'])::TEXT AS data_item_url_mediatree,
			versioned_choices.mesinfo_choice,
			versioned_choices.locuteur_choice,
			versioned_choices.mesinfo_correct,
			versioned_choices.mesinfo_incorrect,
			versioned_choices.speaker_journalist,
			versioned_choices.speaker_commentator,
			versioned_choices.speaker_guest,
			versioned_choices.speaker_politician,
			versioned_choices.speaker_audience,
			versioned_choices.speaker_unknown,
			versioned_choices.mesinfo_corrected,
			versioned_choices.mesinfo_corrected_bool,			
			versioned_choices.debunk_references,
			versioned_choices.claims,
			versioned_choices.explanations,
			versioned_choices.other_comments,
			versioned_choices.annotation_version as "Annotation Version"
		FROM
			labelstudio_task_aggregate
		LEFT JOIN versioned_choices on labelstudio_task_aggregate.task_aggregate_id=versioned_choices.task_aggregate_id
		WHERE 
			((versioned_choices.mesinfo_choice is not null and labelstudio_task_aggregate.is_labeled) or (versioned_choices.mesinfo_choice is null and not labelstudio_task_aggregate.is_labeled))
	) tmp
),
env_shares as (
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
tmp as (
	select 
		tgc.*,
		env_shares.sum_duration_minutes,
		env_shares.weekly_perc_climat
	from 
		tgc
	left join 
		env_shares
	on 
		date_trunc('week', tgc.data_item_start)=env_shares.start
		and tgc.data_item_channel_name=env_shares.channel_name
)
select * from tmp 

