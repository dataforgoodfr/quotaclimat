{{ config(
    materialized='incremental'
    ,unique_key=['id']
  )
}}

with clean_keywords AS (
      SELECT
        "public"."keywords"."id" AS "id",
        json_array_elements(
          "public"."keywords"."keywords_with_timestamp" :: json
        ) AS kw
      FROM
        "public"."keywords"
	 WHERE
        "public"."keywords"."start" >= '2025-01-01'
		AND "public"."keywords"."number_of_keywords" > 0
		AND "public"."keywords"."country" = 'france'
		AND "public"."keywords"."channel_title" <> 'C8'
    ),
	
filtered_keywords AS (
SELECT
	*
FROM clean_keywords  
INNER JOIN "public"."dictionary" 
    ON "public"."dictionary"."keyword" = clean_keywords.kw ->> 'keyword' 
    AND "public"."dictionary"."theme" LIKE clean_keywords.kw ->> 'theme' || '%' -- ensure matc with indirect theme inside the dictionary table
WHERE
    "public"."dictionary"."keyword" IN (
        'acidification des océans',
        'acidification des oceans',
        'algues vertes',
        'aménagement résilient',
        'chalut',
        'chalutage',
        'chalutier',
        'conservation marine',
        'deep sea mining',
        'dessalement de l’eau de mer',
        'élévation du niveau de la mer',
        'élévation du niveau des océans',
        'érosion des côtes',
        'érosion du littoral',
        'exploitation fonds marins',
        'exploitation gazière',
        'exploitation pétrolière',
        'filets de pêche',
        'filets maillants',
        'gestion du littoral',
        'halieutique',
        'hausse du niveau de la mer',
        'hausse du niveau des océans',
        'industrie de la pêche',
        'journée mondiale des océans',
        'limiter l’érosion des côtes',
        'littoral',
        'macro déchet plastique',
        'mer',
        'micro déchet plastique',
        'montée du niveau de la mer',
        'montée du niveau des océans',
        'nano plastique',
        'océan',
        'océanographe',
        'palangre',
        'parc naturel marin',
        'pêche artisanale',
        'pêche au large',
        'pêche côtière',
        'pêche durable',
        'pêche industrielle',
        'pêche professionnelle',
        'pêche responsable',
        'pêcheur',
        'petite pêche',
        'plan de prévention des risques littoraux',
        'pollution de la mer',
        'protection des côtes',
        'protection des océans',
        'quota de pêche',
        'réchauffement des océans',
        'recul du trait de côte',
        'septième continent',
        'stress thermique',
        'système de drainage',
        'surpêche',
        'the metals company',
        'zone marine protégée',
        'zone maritime'
    )
),

distinct_kw AS (
  SELECT
	DISTINCT(id) AS "distinct_id"
  FROM
	filtered_keywords
)

SELECT
  "public"."keywords"."id",
  "public"."keywords"."start",
  "public"."keywords"."channel_title",
  "public"."keywords"."plaintext",
  "public"."keywords"."number_of_keywords",
  "public"."keywords"."keywords_with_timestamp",
  "public"."keywords"."country",
  "public"."keywords"."channel_name"
FROM
  "public"."keywords"
INNER JOIN distinct_kw ON distinct_kw.distinct_id = "public"."keywords".id
WHERE
  "public"."keywords"."start" >= '2025-01-01'
  AND "public"."keywords"."number_of_keywords" > 0
  AND "public"."keywords"."country" = 'france'
  AND "public"."keywords"."channel_title" <> 'C8'
  AND "public"."keywords"."channel_title" IS NOT NULL
  AND "public"."keywords"."channel_title" <> ''