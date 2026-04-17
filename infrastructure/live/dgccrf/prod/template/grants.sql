-- Allow the user to access the schema
GRANT USAGE ON SCHEMA advertising TO :db_user;

-- Write on the two specific tables in the advertising schema only
GRANT INSERT, UPDATE, DELETE ON advertising.ad TO :db_user;
GRANT INSERT, UPDATE, DELETE ON advertising.ad_occurences TO :db_user;