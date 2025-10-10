# QuotaClimat x Data For Good - ![badge](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/polomarcus/579237daab71afbb359338e2706b7f36/raw/test.json)


![](quotaclimat/utils/coverquotaclimat.png)
The aim of this work is to deliver a tool to a consortium around [QuotaClimat](https://www.quotaclimat.org/ "Quotaclimat website"), [Climat Medias](https://climatmedias.org/) allowing them to quantify the media coverage of the climate crisis. 

Radio and TV data are collected thanks to Mediatree API.

And webpress is currently at work in progress (as for 04/2024)

- 2022-09-28, Introduction by Eva Morel (Quota Climat): from 14:10 to 32:00 https://www.youtube.com/watch?v=GMrwDjq3rYs
- 2022-11-29 Project status and prospects by Estelle Rambier (Data): from 09:00 to 25:00 https://www.youtube.com/watch?v=cLGQxHJWwYA
- 2024-03 Project tech presentation by Paul Leclercq (Data) : https://www.youtube.com/watch?v=zWk4WLVC5Hs

## Index
- [I want to contribute! Where do I start?](#contrib)
- [Development](#wrench-development)
  - [File Structure](#file_folder-file-structure)
  - [Setting up the environment](#nut_and_bolt-setting-up-the-environment)

# 🤱 I want to contribute! Where do I start?

1. Learn about the project by watching the introduction videos mentioned above.
2. Create an issue or/and join https://dataforgood.fr/join and the Slack #offseason_quotaclimat.
3. Introduce yourself on Slack #offseason_quotaclimat

##  :wrench: Development

## Contributing

### :nut_and_bolt: Setting up the environment
Doing the following step will enable your local environement to be aligned with the one of any other collaborator.

First install pyenv:

<table>
<tr>
<td> OS </td> <td> Command </td>
</tr>

<tr>
<td> MacOS </td>
<td>

```bash
cd -
brew install pyenv # pyenv itself
brew install pyenv-virtualenv # integration with Python virtualenvsec
```
</td>
</tr>

<tr>
<td> Ubuntu </td>
<td>

```bash
sudo apt-get update; sudo apt-get install make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

curl https://pyenv.run | bash
```
</td>
</tr>

<tr>
<td> Windows </td>
<td>
An installation using miniconda is generally simpler than a pyenv one on Windows.
</td>
</tr>
</table>

Make the shell pyenv aware:

<table>
<tr>
<td> OS </td> <td> Command </td>
</tr>

<tr>
<td> MacOS </td>
<td>

```bash
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

</td>
</tr>

<tr>
<td> Ubuntu </td>
<td>

```bash
export PYENV_ROOT="$HOME/.pyenv"
command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```
</td>
</tr>

<tr>
<td> Windows </td>
<td>

:fr: Dans Propriétés systèmes > Paramètres système avancés >  Variables d'environnement...
Choisissez la variable "Path" > Modifier... et ajoutez le chemin de votre installation python, où se trouve le python.exe. (par défaut, C:\Users\username\AppData\Roaming\Python\Scripts\ )

:uk: In System Properties > Advanced >  Environment Variables...
Choose the variable "Path" > Edit... et add the path to your python's installation, where is located the pyhton.exe (by default, this should be at C:\Users\username\AppData\Roaming\Python\Scripts\ )

In the console, you can now try :
```bash
poetry --version
```

</td>
</tr>
</table>



Let's install a python version (for windows, this step have been done with miniconda):
```bash
pyenv install 3.11.6 # this will take time
```
Check if it works properly, this command:
```bash
pyenv versions
```
should return:
```bash
  system
  3.11.6
```

Then you are ready to create a virtual environment. Go in the project folder, and run:
```bash
  pyenv virtualenv 3.11.6 quotaclimat
  pyenv local quotaclimat
```

In case of a version upgrade you can perform this command to switch
```
eval "$(pyenv init --path)"
pyenv activate 3.11.6/envs/quotaclimat
```

You now need a tool to manage dependencies. Let's use poetry.
On windows, if not already installed, you will need a VS installation.

Link : https://wiki.python.org/moin/WindowsCompilers#Microsoft_Visual_C.2B-.2B-_14.x_with_Visual_Studio_2022_.28x86.2C_x64.2C_ARM.2C_ARM64.29

```bash
pip install poetry
poetry update
poetry lock
```
NLDA : I have not been able to work with wordcloud on windows. 

When you need to install a new dependency (use a new package, e.g. nltk), run 
```bash
poetry add ntlk
```
Update dependencies
```
poetry self update
```

After commiting to the repo, other team members will be able to use the exact same environment you are using. 

## Docker
First, have docker and compose [installed on your computer](https://docs.docker.com/compose/install/#installation-scenarios)

Then to start the different services
```
## To run only one service, have a look to docker-compose.yml and pick one service :
docker compose up metabase
docker compose up ingest_to_db
docker compose up mediatree
docker compose up test
```

### docker secrets
inside the "secrets" folder, you should have these 4 files, you can put dummy values or ask Quota Climat team for the real ones.
```
secrets: # https://docs.docker.com/compose/use-secrets/
  pwd_api:
    file: secrets/pwd_api.txt
  username_api:
    file: secrets/username_api.txt
  bucket:
    file: secrets/scw_bucket.txt
  bucket_secret:
    file: secrets/scw_bucket_secret.txt
```

If you add a new dependency, don't forget to rebuild
```
docker compose build test # or ingest_to_db, mediatree etc
```
### Explore postgres data using Metabase - a BI tool
```
docker compose up metabase -d
```
Will give you access to Metabase to explore the SQL table `sitemap table` or `keywords` here : http://localhost:3000/

To connect to it you have use the variables used inside `docker-compose.yml` :
* password: password
* username: user
* db: barometre
* host : postgres_db

#### Production metabase
If we encounter [a OOM error](https://www.metabase.com/docs/latest/troubleshooting-guide/running.html#heap-space-outofmemoryerrors), we can set this env variable : `JAVA_OPTS=-Xmx2g`

### Web Press - How to scrap 
The scrapping of sitemap.xml is done using the library [advertools.](https://advertools.readthedocs.io/en/master/advertools.sitemaps.html#)

A great way to discover sitemap.xml is to check robots.txt page of websites : https://www.midilibre.fr/robots.txt

What medias to parse ? This [document](https://www.culture.gouv.fr/Thematiques/Presse-ecrite/Tableaux-des-titres-de-presse-aides2) is a good start.

Learn more about [site maps here](https://developers.google.com/search/docs/crawling-indexing/sitemaps/news-sitemap?visit_id=638330401920319694-749283483&rd=1&hl=fr).

#### Scrap every sitemaps
By default, we use a env variable `ENV` to only parse from localhost. If you set this value to another thing that `docker` or `dev`, it will parse everything.

## Test
Thanks to the nginx container, we can have a local server for sitemap :
* http://localhost:8000/sitemap_news_figaro_3.xml

```
docker compose up -d nginx # used to scrap sitemap locally - a figaro like website with only 3 news
# docker compose up test with entrypoint modified to sleep
# docker exec test bash
pytest -vv --log-level DEBUG test # "test" is the folder containing tests
# Only one test
pytest -vv --log-level DEBUG -k detect
# OR
docker compose up test # test is the container name running pytest test
```

## Deploy
Every commit on the `main` branch will build an deploy to the Scaleway container registry a new image that will be deployed. Have a look to `.github/deploy-main.yml`.

Learn [more here.](https://www.scaleway.com/en/docs/tutorials/use-container-registry-github-actions/)

## Monitoring
With Sentry, with env variable `SENTRY_DSN`.

Learn more here : https://docs.sentry.io/platforms/python/configuration/options/

## Mediatree - Import data
Mediatree Documentation API : https://keywords.mediatree.fr/docs/

You must contact QuotaClimat team to 2 files with the API's username and password inside : 
* secrets/pwd_api.txt
* secrets/username_api.txt

Otherwise, a mock api response is available at https://github.com/dataforgoodfr/quotaclimat/blob/main/test/sitemap/mediatree.json

You can check the API with
```
curl -X POST https://keywords.mediatree.fr/api/auth/token/ \
               -H "Content-Type: application/x-www-form-urlencoded" \
               -d "grant_type=password" \
               -d "username=USERNAME" \
               -d "password=PASSWORD"
```

```
curl -X GET "https://keywords.mediatree.fr/api/epg/?channel=tf1&start_gte=2024-09-01T00:00:00&start_lte=2024-09-01T23:59:59&token=TOKEN_RECEIVED_FROM_PREVIOUS_QUERY"
```


### Run
```
docker compose up mediatree
```

### Configuration - Batch import
### Based on time
If our media perimeter evolves, we have to reimport it all using env variable `START_DATE` like in docker compose (epoch second format : 1705409797). By default, it will import 1 day, you can modify it with `NUMBER_OF_PREVIOUS_DAYS` (integer).

Otherwise, default is yesterday midnight date (default cron job).

#### Production safety nets
As Scaleway Serverless service can be down, if some dates are missing until today, it will start back from the latest date saved until today.

### Replay data
When dictionary change, we have to replay our data to update already saved data.
**As pandas to_sql with a little tweak can use upsert (update/insert)**, if we want to update already saved rows, we have to use :
* `START_DATE` 
* `NUMBER_OF_PREVIOUS_DAYS` 

For example to replay data from 2024-05-30 to 2024-05-01 we do from docker compose job "mediatree" (or scaleway job):
* `START_DATE` with unix timestamp of 2024-05-30 (1717020556)
* `NUMBER_OF_PREVIOUS_DAYS` to 30 to get back to 2024-05-01.

**Warning**: it might take several hours.

### Based on channel
Use env variable `CHANNEL` like in docker compose (string: tf1)

Otherwise, default is all channels

### Update without querying Mediatre API
In case we have a new word detection logic - and already saved data from Mediatree inside our DB (otherwise see Batch import based on time or channel) - we can re-apply it to all saved keywords inside our database.

⚠️ in this case, as we won't requery Mediatree API so we can miss some chunks, but it's faster. Choose wisely between importing/updating.

We should use env variable `UPDATE`  like in docker compose (should be set to "true")

In order to see actual change in the local DB, run the test first `docker compose up test` and then these commands :
```
docker exec -ti quotaclimat-postgres_db-1 bash # or docker compose exec postgres_db bash
psql -h localhost --port 5432 -d barometre -U user
--> enter password : password
UPDATE keywords set number_of_keywords=1000 WHERE id = '71b8126a50c1ed2e5cb1eab00e4481c33587db478472c2c0e74325abb872bef6';
UPDATE keywords set number_of_keywords=1000 WHERE id = '975b41e76d298711cf55113a282e7f11c28157d761233838bb700253d47be262';
```

After having updated `UPDATE` env variable to true inside docker-compose.yml and running `docker compose up mediatree` you should see these logs : 
```
 update_pg_keywords.py:20 | Difference old 1000 - new_number_of_keywords 0
```

We can adjust batch update with these env variables (as in the docker-compose.yml): 
```
BATCH_SIZE: 50000 # number of records to update in one batch
```
### Update only one channel 
Use env variable `CHANNEL` like in docker compose (string: tf1) with `UPDATE` to true

### Batch program data
`UPDATE_PROGRAM_ONLY` to true will only update program metadata, otherwise, it will update program metadata and all theme/keywords calculations.

`UPDATE_PROGRAM_CHANNEL_EMPTY_ONLY` to true will only update program metadata with empty value : "".

### Batch update from a date
With +1 millions rows, we can update from an offset to fix a custom logic by using `START_DATE_UPDATE` (YYYY-MM-DD - default first day of the current month), the default will use the end of the month otherwise you can specify `END_DATE` (optional) (YYYY-MM-DD) to batch update PG from a date range.

Env variables list : 
* START_DATE_UPDATE : string (YYYY-MM-DD ) - default to today - minus NUMBER_OF_DAYS (date is included in the query)
* END_DATE : string (YYYY-MM-DD ) - default to end of the month (date is included in the query)
* NUMBER_OF_DAYS : integer default to 7 days - number of days to update from (START_DATE_UPDATE - NUMBER_OF_DAYS) until START_DATE_UPDATE if START_DATE_UPDATE is empty
* STOP_WORD_KEYWORD_ONLY: boolean, default to False. If true will only update rows whose plaintext match top stop words' keyword. It uses to speed up update.
* BIODIVERSITY_ONLY: boolean (default=false), if true will only update rows that have at least one number_of_biodiversity_* > 0

Example inside the docker-compose.yml mediatree service -> START_DATE_UPDATE: 2024-04-01 - default END_DATE will be 2024-04-30
 
We can use [a Github actions to start multiple update operations with different date, set it using the matrix](https://github.com/dataforgoodfr/quotaclimat/blob/main/.github/workflows/scaleway-start-import-job-update.yml)


#### Production executions
~55 minutes to update 50K rows on a mVCPU 2240 - 4Gb RAM on Scaleway.
Every month has ~80K rows.

## SQL Tables evolution
Using [Alembic](https://alembic.sqlalchemy.org/en/latest/autogenerate.html) Auto Generating Migrations¶ we can add a new column inside `models.py` and it will automatically make the schema evolution :

```
# If changes have already been applied (on your feature vranch) and you have to recreate your alembic file by doing :
# 1. change to your main branch 
git  switch main
# 2. start test container (docker compose up testconsole -d / docker compose exec testconsole bash) and run "pytest -vv -k api" to rebuild the state of the DB (or drop table the table you want) - just let it run a few seconds.
# 3. rechange to your WIP branch 
git switch -
# 4. connect to the test container : docker compose up testconsole -d / docker compose exec testconsole bash
# 5. reapply the latest saved state : 
poetry run alembic stamp head
# 6. Save the new columns
poetry run alembic revision --autogenerate -m "Add new column test for table keywords"
# this should generate a file to commit inside "alembic/versions"
# 7. to apply it we need to run, from our container
poetry run alembic upgrade head
```

Inside our Dockerfile_api_import, we call this line 
```
# to migrate SQL tables schema if needed
RUN alembic upgrade head
```
### Channel metadata
In order to maintain channel perimeter (weekday, hours) up to date, we save the current version inside `postgres/channel_metadata.json`, if we modify this file the next deploy will update every lines of inside Postgresql table `channel_metadata`.

## Keywords
## Produce keywords list from Excel files
How to update `quotaclimat/data_processing/mediatree/keyword/keyword.py` from shared excel files ?
Download files locally to "document-experts" from Google Drive (ask on Slack) then :

Macro category sheet must be downloaded as a TSV as Dictionnaire - OME.xlsx - Catégories Transversales.tsv.

```
# Be sure to have updated the folder "document-experts" before running it :
poetry run python3 transform_excel_to_json.py
```

## Program Metadata table
The media perimeter is defined here : "quotaclimat/data_processing/mediatree/channel_program_data.py"

To evolve the media perimeter, we use `program_grid_start` and `program_grid_end` columns to version all evolutions.

To calculate the right total duration for each channel, after updating "quotaclimat/data_processing/mediatree/channel_program_data.py" you need to execute this command to update `postgres/program_metadata.json` 
```
poetry run python3 transform_program.py
```
The SQL queries are based on this file that generate the Program Metadata table.

Program data will not be updated to avoid lock concurrent issues when using `UPDATE=true` for keywords logic. Note: The default case will update them.

**With the docker-entrypoint.sh this command is done automatically, so for production uses, you will not have to run this command.**

# Mediatre to S3
For a security nets, we have configured at data pipeline from Mediatree API to S3 (Object Storage Scaleway) with partition :
* country/year/month/day/channel
If France, country code is None for legacy purposes.

Env variable used :
* START_DATE (integer) (unixtimestamp such as mediatree service)
* NUMBER_OF_PREVIOUS_DAYS (integer): default 7 days to check if something missing
* CHANNEL: (such as mediatree service)
* BUCKET : Scaleway Access key
* BUCKET_SECRET : Scaleway Secret key
* BUCKET_NAME
* DEFAULT_WINDOW_DURATION: int (default=20), the time window to divide the mediatree's 2 minute chunk (must be 120 secondes / DEFAULT_WINDOW_DURATION == 0)
* COUNTRY : 3 letter country code (default = fra - [Source](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3)), see country.py to see them all - to get all countries the code is "all". 

# Stop words
To prevent advertising keywords to blow up statistics, we remove stop words based on the number of times a keyword is said in the same context.

The result will be saved inside postgresql table: stop_word.

This table is read by the service "mediatree" to remove stop words from the field "plaintext" to avoid to count them.

Env variables used : 
* START_DATE (integer) (unixtimestamp such as mediatree service)
* NUMBER_OF_PREVIOUS_DAYS (integer): default 7 days
* MIN_REPETITION (integer) : default 15 - Number of minimum repetition of a stop word
* CONTEXT_TOTAL_LENGTH (integer) : default 80 - the length of the advertising context (sentence) saved
* FILTER_DAYS_STOP_WORD (integer): default 30 - number of days to filter the last stop words saved from - to speed up update execution
 
## Remove a stop word
To remove a false positive, we set to false the `validated` attribute :
```
docker exec -ti quotaclimat-postgres_db-1 bash # or docker compose exec postgres_db bash
psql -h localhost --port 5432 -d barometre -U user
--> enter password : password
UPDATE stop_word set validated=false WHERE id = 'MY_ID';
```

## Production monitoring
* Use scaleway
* Use [Ray dashboard] on port 8265

## Bump version
[poetry bump](https://python-poetry.org/docs/cli/#version)
```
poetry version minor
```

## Materialized view - dbt
We can define some slow queries to make them efficient with materialized views using [DBT](https://www.getdbt.com/), used via docker :
```
docker compose up testconsole -d
docker compose exec testconsole bash
> dbt debug  # check if this works
# caution: this seed will reinit the keywords and program_metadata tables
> dbt seed --select program_metadata --select keywords --full-refresh  # will empty your local db - order is important
> dbt run --models homepage_environment_by_media_by_month # change by your file name
> poetry run pytest --log-level DEBUG -vv my_dbt_project/pytest_tests # unit test 
```

**Protips**: [Explore these data with postgres data using Metabase locally](https://github.com/dataforgoodfr/quotaclimat?tab=readme-ov-file#explore-postgres-data-using-metabase---a-bi-tool)

### DBT production
To update monthly our materialized view in production we have to use this command ([automatically done inside our docker-entrypoint](https://github.com/dataforgoodfr/quotaclimat/blob/main/docker-entrypoint.sh#L17)) that is run on every deployement of api-import (daily) :
```
poetry run dbt run --full-refresh
```

#### Causal query - too slow
Because this query is too massive, we set it month by month and avoid using a full-refresh. See units tests and docker-entrypoint.sh to see how.

If we change the DBT code, we have to relaunch this command to have a refreshed view (or wait the next daily cron).

### SRT to Mediatree Format
Some Speech to Text data come from other sources than Mediatree, so we have to transform those source into the mediatree format to process them.

#### Run germany
Only for german data using parquet
```
docker compose up srt
```
or
```bash
docker compose up testconsole -d
docker compose exec testconsole bash
/app/ cd i8n/
/app/i8n# poetry run python3 srt-to-mediatree-format-parquet.py
```
#### Run belgium
Only for belgian data using .csv

Warning: this job is not automated as the process depending on getting the data is manual (emails), so we have to modify [the script here](https://github.com/dataforgoodfr/quotaclimat/blob/01e5ede5152d4113c68bcf994f13c7b2baa30dd6/i8n/srt-to-mediatree-format.py#L259-L262).
```
docker compose up testconsole -d
docker compose exec testconsole bash
/app/ cd i8n/
/app/i8n# poetry run python3 srt-to-mediatree-format.py
```

### Fix linting
Before committing, make sure that the line of codes you wrote are conform to PEP8 standard by running:
```bash
poetry run black .
poetry run isort .
poetry run flake8 .
```
There is a debt regarding the cleanest of the code right now. Let's just not make it worth for now.

# Labelstudio
For the Climate Safeguards project we ingest the data present in the Labelstudio databases into the Barometre database. This way we can analyse the annotations of the factcheckers and extract key insights. There are two main tables that we need to ingest:
* `task`
* `task_completion`
The `task` table contains an item (a 2 minute segment) with some metadata and an ID, and the `task_completion` table contains the annotations for that task. As the Labelstudio databases are separate from each other, we create a `labelstudio_task_aggregate` table and `labelstudio_task_completion_aggregate` table to perform a union of all the tasks and annotations. We also create `task_aggregate_id` and `task_completion_aggregate_id` to uniquely identify each task and annotation based on a hash of the table's id, project_id column and country column.
## SQLAlchemy models
The models for the two tables can be found in `quotaclimat/data_ingestion/labelstudio/models.py`, any update to these models can be tracked with `alembic`, as the migration tool has been setup to track `TargetBase` as well as the already existing tables.
## Source configuration
The sources for the Labelstudio ingestion can be found in `quotaclimat/data_ingestion/labelstudio/configs.py`. The `db_config` variable consists of a list of record (python dictionaries) with the source database name and a mapping of the project ids to countries:
```python
db_config = [
  {
    "database": "<labelstudio_db>", 
    "countries": {
      1: "<country_1>", 
      2: "<country_2>", 
      3: "<country_3>",
    }
  },
]
```
When a new source is added (if a new Labelstudio instance is deployed) it suffices to add the source to the record list. (This assumes that all sources are on the same DB instace, as is the case at the time of writing).
## Local execution
In order to execute the ingestion script locally you will need either have a working labelstudio locally, or to connect to the remote labelstudio with a read-only user.
Set your credentials in the `docker-compose.yml` file:
```
LABELSTUDIO_INGESTION_POSTGRES_USER: <user>
LABELSTUDIO_INGESTION_POSTGRES_PASSWORD: <password>
```
and run the script via the test console:
```bash
docker compose up testconsole -d 
docker compose exec testconsole bash
poetry run python -m quotaclimat.data_ingestion.labelstudio.ingest_labelstudio
```

# Analytics
In order to improve the performance of the dashboards hosted on Metabase, intermediate tables are calculated using `dbt` in the `analytics` schema. These can be found in `my_dbt_project/models/analytics`. These dbt models need to be run using the `--target analytics` command. You can test these locally using the test console:
```bash
docker compose up testconsole -d 
docker compose exec testconsole bash
# Seed the labelstudio tables
poetry run dbt seed --select program_metadatalabelstudio_task_aggregate --select labelstudio_task_completion_aggregate
# run the dbt model on the analytics target
poetry run dbt run --target analytics --select task_global_completion
```
## Thanks
* [Paul Leclercq] (https://www.epauler.fr/)
* [Eleven-Strategy](https://www.welcometothejungle.com/fr/companies/eleven-strategy)
* [Kevin Tessier](https://kevintessier.fr)