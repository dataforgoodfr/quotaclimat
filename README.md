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
2. Launch the Streamlit dashboard on your local, and browse around. Steps are described below.
3.  Check out the data in data_public.
4. Join https://dataforgood.fr/join and the Slack #offseason_quotaclimat
5. Introduce yourself on Slack by stating your expertise and how much time you would like to give. 
6. Join a dev meetings on Tuesdays at 19h or 13h.


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


### Run the dashboard
```bash
poetry run streamlit run app.py
```
On Windows, you may need :
```bash
poetry run python -m streamlit run app.py
```
Depending on your installation process and version, "python" can also be "python3" or "py".

### Fix linting
Before committing, make sure that the line of codes you wrote are conform to PEP8 standard by running:
```bash
poetry run black .
poetry run isort .
poetry run flake8 .
```
There is a debt regarding the cleanest of the code right now. Let's just not make it worth for now.
