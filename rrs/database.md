# Database setup

Table 1
--
schema: `public` <br/>
table name: `subjects`<br/>
columns: <br/>
- subject_id (pk, text)
- name (text)
- created_at (datetime)
- updated_at (datetime)

Table 2
--
schema: `public` <br/>
table name: `dictionary`<br/>
columns: <br/>
- keyword_id (pk, string)
- keyword (string)
- high_risk_false_positive (boolean)
- created_at (datetime)
- updated_at (datetime)

Table 3
--
schema: `public` <br/>
table name: `segments`<br/>
columns: <br/>
- segment_id (pk, string)
- subject_id (fk, string)
- s3_uri (string)
- n_keywords (int)
- created_at (datetime)
- updated_at (datetime)

Table 4
--
schema: `public` <br/>
table name: `cases`<br/>
columns: <br/>
- case_id (pk, string)
- segment_id (fk, string)
- subject_id (fk, string)
- model_score (string)
- model_reason (string)
- created_at (datetime)
- updated_at (datetime)

Table 5
--
schema: `public` <br/>
table name: `clusters`<br/>
columns: <br/>
- cluster_id (pk, string)
- subject_id (fk, string)
- cluster_text (string)
- created_at (datetime)
- updated_at (datetime)

Table 6
--
schema: `public` <br/>
table name: `case_to_clusters`<br/>
columns: <br/>
- case_id (pk, string)
- cluster_id (fk, string)
- created_at (datetime)
- updated_at (datetime)
