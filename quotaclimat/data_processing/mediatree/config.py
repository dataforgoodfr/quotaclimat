import os

#read whole file to a string
def get_password():
    password = os.environ.get("MEDIATREE_PASSWORD")
    if(password == '/run/secrets/pwd_api'):
        password= open("/run/secrets/pwd_api", "r").read()
    return password

def get_auth_url():
    return os.environ.get("MEDIATREE_AUTH_URL") # 

def get_user():
    USER = os.environ.get("MEDIATREE_USER")
    if(USER == '/run/secrets/username_api'):
        USER=open("/run/secrets/username_api", "r").read()
    return USER

#https://keywords.mediatree.fr/docs/#api-Subtitle-SubtitleList
def get_keywords_url():
    return os.environ.get("KEYWORDS_URL") 