{{ 
    config(
        materialized='table',
    )
}}

WITH choice_annotations AS (
	SELECT
		public.labelstudio_task_completion_aggregate.task_completion_aggregate_id as task_completion_aggregate_id,
		public.labelstudio_task_completion_aggregate.task_aggregate_id as task_aggregate_id,
		string_agg(CASE WHEN subquery.choices IN ('Correct', 'Incorrect') THEN subquery.choices END, ',' ORDER BY subquery.choices) AS mesinfo_choice,
		string_agg(
			CASE 
				WHEN subquery.choices IN ('Journalist', 'Commentator', 'Guest', 'Politician', 'Audience', 'Unknown') 
				THEN subquery.choices 
			END,
			',' ORDER BY subquery.choices
		) AS locuteur_choice,
		CASE WHEN SUM(CASE WHEN subquery.choices = 'Correct' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS mesinfo_correct,
		CASE WHEN SUM(CASE WHEN subquery.choices = 'Incorrect' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS mesinfo_incorrect,
		CASE WHEN SUM(CASE WHEN subquery.choices = 'Journalist' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_journalist,
		CASE WHEN SUM(CASE WHEN subquery.choices = 'Commentator' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_commentator,
		CASE WHEN SUM(CASE WHEN subquery.choices = 'Guest' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_guest,
		CASE WHEN SUM(CASE WHEN subquery.choices = 'Politician' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_politician,
		CASE WHEN SUM(CASE WHEN subquery.choices = 'Audience' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_audience,
		CASE WHEN SUM(CASE WHEN subquery.choices = 'Unknown' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS speaker_unknown
	FROM
		public.labelstudio_task_completion_aggregate
	LEFT JOIN LATERAL (
		SELECT
			jsonb_array_elements_text(elem -> 'value' -> 'choices') AS choices
		FROM
			jsonb_array_elements(public.labelstudio_task_completion_aggregate.result::jsonb) AS elem
		WHERE
			elem ->> 'type' = 'choices'
	) 
	subquery ON true
	GROUP BY task_completion_aggregate_id, task_aggregate_id
),
versioned_choices AS (
	SELECT
        ROW_NUMBER() OVER (PARTITION BY choice_annotations.task_aggregate_id ORDER BY choice_annotations.task_completion_aggregate_id) AS annotation_version,
		choice_annotations.*
	FROM 
		choice_annotations 
)
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
		versioned_choices.annotation_version as "Annotation Version"
	FROM
		labelstudio_task_aggregate
	LEFT JOIN versioned_choices on labelstudio_task_aggregate.task_aggregate_id=versioned_choices.task_aggregate_id
	WHERE 
		((versioned_choices.mesinfo_choice is not null and labelstudio_task_aggregate.is_labeled) or (versioned_choices.mesinfo_choice is null and not labelstudio_task_aggregate.is_labeled))
) tmp
