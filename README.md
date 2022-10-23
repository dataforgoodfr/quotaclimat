# QuotaClimat
![](coverquotaclimat.png)

- Pour rejoindre le projet https://dataforgood.fr/join puis sur le Slack #offseason_quotaclimat
- Pour en savoir plus sur le projet https://dataforgood.fr/projects/quotaclimat
- Pour la répartition des tâches https://dataforgood.slite.page/p/xECA5kt9LFqtOA/Comment-contribuer-au-projet-QuotaClimat-x-Data-For-Good

## :ledger: Index

- [Open source](#open-source)
- [Development](#wrench-development)
  - [File Structure](#file_folder-file-structure)
  - [Setting up the environment](#nut_and_bolt-setting-up-the-environment)

## Open source

Pour l'instant le repo est privé, parce que les données sont privées.
Nous passerons le projet en open source dès que nous aurons réglé la mise en place d'une base de données SQL

##  :wrench: Development

### :file_folder: Repo structure
```
- data --------------------------------- les données temporairement ici
- notebooks ---------------------------- les analyses
        quickstart.ipynb --------------- un premier notebook Python d'analyse
- data_processing ---------------------- methods related to process raw and aggregated data
        read_format_deduplicate -------- read all files and put them together
- data_analytics ----------------------- methods and figures answer the questions from media tree
        data_coverage.py --------------- coverage of extracts and keywords in samples
- ui ----------------------------------- dashboarding
        streamlite_dashboard.py -------- Putting the figure together and set layout
- app.py ------------------------------- Run dashboard
```

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
</table>



Let's install a python version:
```bash
pyenv install 3.10.2 # this will take time
```
Check if it works properly, this command:
```bash
pyenv versions
```
should return:
```bash
  system
  3.10.2
```

Then you are ready to create a virtual environment. Go in the project folder, and run:
```bash
  pyenv virtualenv 3.10.2 quotaclimat
  pyenv local quotaclimat
```

You now need a tool to manage dependencies. Let's use poetry.

```bash
pip install poetry
poetry update
```

When you need to install a new dependency (use a new package, e.g. nltk), run 
```bash
poetry add ntlk
```

After commiting to the repo, other team members will be able to use the exact same environment you are using. 