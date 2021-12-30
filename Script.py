import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import regex as re
import json
from urllib.request import urlopen

#Pofiling cmd: kernprof -l -v "G:\OneDrive\Documents\Projects\GDLive Scraper\Script.py"
### Functions ###


def extract_recipient_data(rid):
    """
    

    Parameters
    ----------
    rid : int
        The recipient ID of the GD-Live profile url.

    Returns
    -------
    profile_data : Pandas DataFrame
        Returns a Pandas DataFrame containing the responses and metadata of a GD-Live profile.

    """
    global url
    url = "https://live.givedirectly.org/newsfeed/a7de23c0-39af-4af3-9671-13dc38a85e26/"+str(rid)+"?context=newsfeed"
 
    profile = load_recipient_profile(url)   
    details = get_recipient_details(profile,key)
    profile_data = get_survey_as_df(profile,"enrollment")

    payments = list(set(re.findall(r"payment_\d{1,2}",source)))
    for payment in payments:
        payment_i = get_survey_as_df(profile,payment)
        profile_data = pd.concat([profile_data,payment_i])

    for column in enumerate(details):
        profile_data.insert(column[0],column[1],details[column[1]])
        
    return profile_data


def load_recipient_profile(recipient_url):
    """
    

    Parameters
    ----------
    recipient_url : str
        A string containing the url of the GD-Live profile.

    Returns
    -------
    BeautifulSoup
        Returns a BeautifulSoup file containing the profile data.

    """
    global source
    source = requests.get(recipient_url).text
    return BeautifulSoup(source, "lxml")


def get_recipient_details(profile,key):
    """
    

    Parameters
    ----------
    profile : BoutifulSoup
        A BeautifulSoup file containing data from a GD-Live profile.
    key: str
        A key for accessing the gender classification tool. Please get your personal API key from gender-api.com
        
    Returns
    -------
    Pandas DataFrame
        Returns the metadata of a GD-Live profile, including "rid","url","name","gender","accuracy","sample", and "final payment received?".

    """
    name = profile.find("div", class_="card-name card-name-profile").text.strip()
    
    api_url = "https://gender-api.com/get?key=" + key + "&name=" + name
    response = urlopen(api_url)
    decoded = response.read().decode('utf-8')
    data = json.loads(decoded)
    gender = data["gender"]
    accuracy = data["accuracy"]
    sample = data["samples"]
    
    complete = (profile.find_all("div", class_="no-updates-message") != [])
    
    #other details
    details = np.array([item.text.strip() for item in profile.find_all("div", class_="fact-content")])    
    details = np.append(np.array([rid,url,name,gender,accuracy,sample,complete]),details) 
    
    #labels
    labels =  np.array([item.text.strip() for item in profile.find_all("div", class_="fact-label")])
    labels = np.append(np.array(["rid","url","name","gender","accuracy","sample","final payment received?"]),labels)
    
    return pd.DataFrame(details.reshape(-1,len(details)),columns= labels)

#@profile
def get_survey_as_df(profile, survey):
    """
    

    Parameters
    ----------
    profile : BeautifulSoup
        A BeautifulSoup file containing a GD-Live profile.
    survey : str
        A string indicating which profile response to load. These are either 'enrollment' or 'payment_X'.

    Returns
    -------
    TYPE
        Return a Pandas DataFrame containing a survey response from a GD-Live profile.

    """
    answers = np.array(get_profile_item(profile, survey,"survey-answer"))
    amount = get_profile_item(profile, survey,"transfer-amount-content")
    year = get_profile_item(profile, survey,"phase-time")
    answers = np.append(np.array([year, amount, survey],dtype=object),answers)
    questions = np.array(get_profile_item(profile, survey,"survey-prompt"))
    questions = np.append(np.array(["year", "amount", "survey"]),questions)
    return pd.DataFrame(answers.reshape(-1,len(answers)),columns= questions)

#@profile
def get_profile_item(soup, container_name, html_class):
    """
    

    Parameters
    ----------
    soup : BeautifulSoup
        A BeautifulSoup file containing a GD-Live profile..
    container_name : str
        A string indicating which profile response to load. These are either 'enrollment' or 'payment_X'.
    html_class : str
        A string indicating which part of the profile response to load. These are either 'survey_answer','transfer-amount-content', 'phase-time', or 'survey-prompt'.

    Returns
    -------
    list
        Returns a list containing a part of a GD-Live profile survey response.

    """
    container = soup.find("div", {"id" : re.compile(container_name)})
    return [item.text.strip() for item in container.find_all("div", class_=html_class)]


#@profile
def execution(start_rid, interval, number, gender_api_key):
    """
    

    Parameters
    ----------
    start_rid : int
        The recipient ID from which to start loading the GD-Live profile. Please not that the minimum is around 158000.
    interval : int
        The interval at which to sample rid's.
    number : int
        The amount of profiles to sample.
    gender_api_key: str
        A key for accessing the gender classification tool. Please get your personal API key from gender-api.com

    Returns
    -------
    df : Pandas DataFrame
        A Pandas DataFrame containing the sampled GD-Live profiles.

    """
    global key
    key = gender_api_key
    df = pd.DataFrame()
    #Start with 158000
    global rid
    rid = start_rid
    for i in range(0,number): 
        try:
            profile_data = extract_recipient_data(rid)
            df = pd.concat([df,profile_data])
            print(rid)
            rid += interval
            
        except AttributeError:
            print("Attribute error at rid "+str(rid))
            rid += interval

        

    df["year"] = df["year"].str[0]
    df["amount"] = df["amount"].str[0]
    df["dollar"] = df["amount"].str.extract(r"\$([1-9]+)")
    df["localfx"] = df["amount"].str.extract(r"(?m)^(\d+).*")
    
    return df

### Execution ###
#Standard Values: execution(158000,1000,52, "key")
#Please get you API key from gender-api.com
sample = execution(158000,1000,5, "key")



   
