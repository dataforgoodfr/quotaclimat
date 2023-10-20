# QuotaClimat x Data For Good
![](quotaclimat/utils/coverquotaclimat.png)
The aim of this work is to deliver a tool to [QuotaClimat](https://www.quotaclimat.org/ "Quotaclimat website"), allowing them to quantify the media coverage of the climate crisis. By the mean of sitemap scrapping (among others data sources), a Streamlit dashboard is developed to answer their needs. 
- 2022-09-28, Introduction by Eva Morel (Quotaclimat): from 14:10 to 32:00 https://www.youtube.com/watch?v=GMrwDjq3rYs
- 2022-11-29 Project status and prospects by Estelle Rambier (Data): from 09:00 to 25:00 https://www.youtube.com/watch?v=cLGQxHJWwYA

## Index
- [I want to contribute! Where do I start?](#contrib)
- [Development](#wrench-development)
  - [File Structure](#file_folder-file-structure)
  - [Setting up the environment](#nut_and_bolt-setting-up-the-environment)

# 🤱 I want to contribute! Where do I start?

1. Learn about the project by watching the introduction videos mentioned above.
3. Check out the data in data_public.
4. Join https://dataforgood.fr/join and the Slack #offseason_quotaclimat.
2. Ask access to the [documentation](https://www.notion.so/dataforgood/QuotaClimat-6c011dc529f14f309f74970df243b819) to Estelle Rambier (dm on Slack).
5. Introduce yourself on Slack #offseason_quotaclimat.
6. Join a dev meetings on Tuesdays at 19h, you will be able to view what's currently going on and we will find a something for you to contribute. If you can't make it on Tuesdays, send a Slack dm to Estelle Rambier.


##  :wrench: Development

### :file_folder: Repo structure
```
-.github/workflows --------------------- ochestrate GH actions jobs
- data_public -------------------------- data ingested by the scrapping jobs
- notebooks ---------------------------- r&d
        COP27/ ------------------------- COP27 notebook analysis
- quotaclimat -------------------------- all methods needed to serve the dashboard
        data_processing ---------------- methods related to process raw and aggregated data
        data_ingestion ----------------- scripts for scrapping jobs
        data_models -------------------- data schemas
        data_analytics ----------------- methods and figures answer the questions from media tree
        utils --------------------------
            plotly_theme.py ------------ visual identity of the project's figures
- pages -------------------------------- the different pages making the dashboard
app.py --------------------------------- run dashboard
```
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
```
NLDA : I have not been able to work with wordcloud on windows. 

When you need to install a new dependency (use a new package, e.g. nltk), run 
```bash
poetry add ntlk
```

After commiting to the repo, other team members will be able to use the exact same environment you are using. 

## Docker
First, have docker and compose [installed on your computer](https://docs.docker.com/compose/install/#installation-scenarios)

Then to start the different services
```
## To run only one service, have a look to docker-compose.yml and pick one service :
docker compose up sitemap_app
docker compose up ingest_to_db
docker compose up streamlit
```

### Explore postgres data using Metabase - a BI tool
```
docker compose up metabase
```

Will give you access to Metabase to explore the SQL table `sitemap table` here : http://localhost:3000/

To connect to it you have use the variables used inside `docker-compose.yml` :
* password: password
* username: user
* db: barometre
* host : postgres_db
#### Production metabase
If we encounter [a OOM error](https://www.metabase.com/docs/latest/troubleshooting-guide/running.html#heap-space-outofmemoryerrors), we can set this env variable : `JAVA_OPTS=-Xmx2g`

### Run the dashboard
```bash
poetry run streamlit run app.py
```
On Windows, you may need :
```bash
poetry run python -m streamlit run app.py
```
Depending on your installation process and version, "python" can also be "python3" or "py".

### How to scrap 
The scrapping of sitemap.xml is done using the library [advertools.](https://advertools.readthedocs.io/en/master/advertools.sitemaps.html#)

Learn more about [site maps here](https://developers.google.com/search/docs/crawling-indexing/sitemaps/news-sitemap?visit_id=638330401920319694-749283483&rd=1&hl=fr).

#### Scrap every sitemaps
By default, we use a env variable `ENV` to only parse from localhost. If you set this value to another thing that `docker` or `dev`, it will parse everything.

## Test
Thanks to the nginx container, we can have a local server for sitemap :
* http://localhost:8000/sitemap_news_figaro_3.xml


```
docker compose up -d nginx # used to scrap sitemap locally - a figaro like website with only 3 news
pytest test # "test" is the folder containing tests
# OR
docker compose up test # test is the container name running pytest test
```

## Deploy
Every commit on the `main` branch will build an deploy to the Scaleway container registry a new image that will be deployed. Have a look to `.github/deploy-main.yml`.

Learn [more here.](https://www.scaleway.com/en/docs/tutorials/use-container-registry-github-actions/)

### Fix linting
Before committing, make sure that the line of codes you wrote are conform to PEP8 standard by running:
```bash
poetry run black .
poetry run isort .
poetry run flake8 .
```
There is a debt regarding the cleanest of the code right now. Let's just not make it worth for now.
