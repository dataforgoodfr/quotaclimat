# QuotaClimat
![](coverquotaclimat.png)

- Pour rejoindre le projet https://dataforgood.fr/join puis sur le Slack #offseason_quotaclimat
- Pour en savoir plus sur le projet https://dataforgood.fr/projects/quotaclimat
- Pour la répartition des tâches https://dataforgood.slite.page/p/xECA5kt9LFqtOA/Comment-contribuer-au-projet-QuotaClimat-x-Data-For-Good

## Structure du repo
```
- data --------------------------------- les données temporairement ici
- notebooks ---------------------------- les analyses
        quickstart.ipynb --------------- un premier notebook Python d'analyse
```

## Open source

Pour l'instant le repo est privé, parce que les données sont privées.
Nous passerons le projet en open source dès que nous aurons réglé la mise en place d'une base de données SQL

## Setting up the environement
Doing the following step will enable your local environement to be aligned with the one of any other collaborator.
First install pyenv:
```bash
cd -
brew install pyenv # pyenv itself
brew install pyenv-virtualenv # integration with Python virtualenvsec
```

Make the shell pyenv aware:
```bash
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

Let's install a python version:
```bash
pyenv install 3.10.2 # this will take time
```
Check if it works properly, this :
```bash
pyenv versions
```
should return 
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
poetry update
```

When you need to install a new dependency (use a new package, e.g. nltk), run 
```bash
poetry add ntlk
```

After commiting to the repo, other team members will be able to use the exact same environment you are using. 