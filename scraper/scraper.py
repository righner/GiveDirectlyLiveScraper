import sys
import requests
from bs4 import BeautifulSoup
import regex as re
import hashlib
#import flair

#dask
import dask


#logging
from dask.diagnostics import ProgressBar
pbar = ProgressBar()                
pbar.register() # global registration
import logging
#logging.basicConfig(filename=str(datetime.now().strftime('%Y-%m-%dT%H-%M-%S'))+'_scraper.log', encoding='utf-8', level=logging.INFO)
#logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

#time parsing
from datetime import datetime
from dateutil.relativedelta import relativedelta


from timer import Timer

#Pofiling cmd: kernprof -l -v "G:\OneDrive\Documents\Projects\GDLive Scraper\Script.py"
 

### Functions ###
  
def create_payloads(dask_product):
    recipients_payload = []
    responses_payload = []
    loaded = 0
    no_profile = 0
    parsing_error = 0
    unknown_error = 0
    no_updates = 0
    no_questions = 0
    empty_response = 0
    logging.info("Parsing payload...")
    #logging.info(dask_product)
    for load in dask_product:
        #logging.info(load[1])
        try:
            if load == "Profile does not exist":
                no_profile += 1
            elif load == "Error":
                unknown_error += 1
            elif load[1] == ["No updates"]:
                no_updates += 1
            elif load[1] == ["No questions asked"]:
                no_questions += 1   
            elif load[1] == ["Empty"]:
                empty_response += 1
            elif load[1] == None or load[0] == None:
                unknown_error += 1
            else:
                try:
                    recipients_payload = [*recipients_payload,*load[0]]
                    responses_payload = [*responses_payload,*load[1]]
                    #logging.info(responses_payload)
                    loaded += 1
                except Exception as inner:
                    logging.warning("   Error while parsing this load:"+str(load)+"\n     "+str(inner))
                    parsing_error += 1  
                
        except Exception as outer:
            unknown_error += 1
            logging.error("     Unknown error in this load:"+str(load)+"\n     "+str(outer))
            
    return recipients_payload, responses_payload, loaded, no_profile, parsing_error, no_updates,unknown_error,empty_response, no_questions


def scrape_profile(rid,completed_surveys,sentiment_model):
    
    try:
        
        
        url = "https://live.givedirectly.org/newsfeed/a7de23c0-39af-4af3-9671-13dc38a85e26/"+str(rid)+"?context=newsfeed"

        profile, source = load_recipient_profile(url,rid)
        if source:
            recipient = dask.delayed(get_recipient_details)(profile,rid)

            responses = dask.delayed(get_recipient_surveys)(profile,rid,source,completed_surveys,sentiment_model)
 
            return dask.compute(recipient,responses)
        else:
            return profile

    except Exception as e:
        if "Sorry, the recipient you're looking for can't be found" in str(profile):
            return "Profile does not exist"
        else:
            logging.error("Unknown error at rid "+str(rid)+"\n   "+str(e))
            pass



def get_recipient_surveys(profile,rid,source,completed_surveys,sentiment_model):
    """
    Extracts survey data from a profile and returns  the responses as a json payload.
    It first loads the initial enrollment survey (if given), then checks how many follow up surveys were conducted, and finally extracts and loads each of them into a list.

    Parameters
    ----------
    profile : Beautiful Soup
        A BeautifulSoup file containing data from a GD-Live profile.
    rid : int
        The recipient ID of the GD-Live profile url.
    source : html source code
        Source code containing the profile site.

    Returns
    -------
    responses: List of surveys and their responses in json format

    """
    responses = []
    skip_count = 0
    enrollment_id = str(rid)+"_"+str(0)
    if enrollment_id not in completed_surveys:
        try:
            responses= [*responses,*get_survey_jsons(rid,profile,"enrollment",sentiment_model)] #the '*' operator gets all items from the list
        except AttributeError:
            logging.info("     No survey class 'enrollment' found in rid "+str(rid))
            skip_count += 1
    else:
        logging.info("     Skipped survey "+enrollment_id)
        skip_count += 1

    payments = list(set(re.findall(r"payment_\d{1,2}",source )))
    for payment in payments:
        payment_id = str(rid)+"_"+re.findall(r"([1-9]+)",payment)[0]
        if payment_id not in completed_surveys:
                responses= [*responses,*get_survey_jsons(rid,profile,payment,sentiment_model)] 
                #logging.info("     Payment "+payment+" extracted")
        else:
            logging.info("     Skipped survey "+payment_id)
            skip_count += 1
        
    if skip_count != (1 + len(payments)):
        return responses
    elif skip_count == (1 + len(payments)):
        logging.info("No Updates for rid "+str(rid))
        return ["No updates"]
    elif responses == []:
        logging.warning("Empty response array returned for rid "+str(rid))
        return ["Empty"]



def load_recipient_profile(recipient_url,rid):
    """
    

    Parameters
    ----------
    recipient_url : str
        A string containing the url of the GD-Live profile.

    Returns
    -------
    BeautifulSoup
        Returns a BeautifulSoup containing the profile data.

    """
    try:
        source = requests.get(recipient_url).text
        profile = BeautifulSoup(source, "lxml")
        if "Sorry, the recipient you're looking for can't be found" in str(profile):
            return "Profile does not exist",None
        else:
            logging.info("Data extracted from recipient "+str(rid))
            return profile, source
    except Exception as e:
        logging.error("Failure resquesting from rid "+str(rid)+ "\n     "+str(e))
        raise e        
    


def get_recipient_details(profile,rid):
    """
    

    Parameters
    ----------
    profile : BoutifulSoup
        A BeautifulSoup file containing data from a GD-Live profile.
    rid: int
        The recipient ID of the GD-Live profile url.
        
    Returns
    -------
    recipient: list
        Returns the metadata of a GD-Live profile: recipient_id, name, age, country, occupation, status boolean (completet or not),campaign name, and current timestamp (update time)".

    """
    name = profile.find("div", class_="card-name card-name-profile").text.strip()
    try:
        age = profile.find("div", class_="fact fact-age").find("div", class_="fact-content").text.strip()
    except AttributeError:
        logging.info("     No age for profile "+str(rid))
        age= None
    try:
        country = profile.find("div", class_="fact fact-country").find("div", class_="fact-content").text.strip()
    except AttributeError:
        logging.info("     No country for profile "+str(rid))
        country = None
    try:
        occupation = profile.find("div", class_="fact fact-occupation").find("div", class_="fact-content").text.strip()        
    except AttributeError:
        logging.info("     No occupation for profile "+str(rid))
        occupation = None
    campaign = profile.find("div", class_="fact fact-project").find("div", class_="fact-content").text.strip()
    complete = (profile.find_all("div", class_="no-updates-message") != [])
    
    last_updated = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

  
    #create payload json
    return [{"recipient_id":rid,
        "name":name,
        "age":age,
        "country":country,
        "occupation":occupation,
        "completed":complete,
        "campaign":campaign,
        "last_updated":last_updated}
        ]


#@profile
def get_survey_jsons(rid,profile, survey,sentiment_model):
    """
    Extracts the questions and repsponses of a specific survey from a GDLive profile. 

    Parameters
    ----------
    profile : BeautifulSoup
        A BeautifulSoup file containing a GD-Live profile.
    survey : str
        A string indicating which profile response to load. These are either 'enrollment' or 'payment_X'.

    Returns
    -------
    survey data: List containing json format survey data.

    """
    
    responses = get_profile_item(profile, survey,"survey-answer")
    #logging.info(responses)
    questions = get_profile_item(profile, survey,"survey-prompt")
    #logging.info(questions)


    amount_str = get_profile_item(profile, survey,"transfer-amount-content")
    
    if amount_str == []:
        amount = None
        local_amount = None
    else:
        try:        
            amount = re.findall(r"\$([1-9]+)",amount_str[0])[0]
        except Exception:
            logging.error("Dollar amount missing for rid "+str(rid)+"\n"+str(amount_str))
            amount = None
        try:        
            local_amount = re.findall(r"(?m)^(\d+).*",amount_str[0])[0]
        except Exception:
            logging.error("Local amount missing for rid "+str(rid)+"\n"+str(amount_str))
            local_amount = None

    if survey == "enrollment":
        payment = 0
    else:
        payment = int(re.findall(r"([1-9]+)",survey)[0])

    timestamp = get_profile_item(profile, survey,"phase-time")[0]
    try:
        year = parse_timestamp(timestamp)
    except UnboundLocalError as e:
        logging.info("     Unknown time format at rid "+str(rid))
        raise e

    


    responses_list = []
    for (response, question) in zip(responses,questions):
        response_id = hashlib.md5(bytes(str(rid)+survey+response, 'utf-8')).hexdigest()
        #sentence = flair.data.Sentence(response)
        #sentiment_model.predict(sentence)
        #logging.info(sentence)
        responses_list.append(
            {"response_id":response_id,
            "recipient_id":rid,
            "year":year,
            "payment":payment,
            "usdollar":amount,
            "localfx":local_amount,
            "question":question,           
            "response":response})
    try:
        #logging.info(responses_list)
        return responses_list
    except IndexError:
        logging.info("     Likely no survey questions asked for "+survey+" in "+str(rid))
        return "No questions asked"

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
        A string indicating which part of the profile response to load. These are either 'survey_response','transfer-amount-content', 'phase-time', or 'survey-prompt'.

    Returns
    -------
    list
        Returns a list containing a part of a GD-Live profile survey response.

    """

    container = soup.find("div", {"id" : re.compile(container_name)})
    return [item.text.strip() for item in container.find_all("div", class_=html_class)]

def parse_timestamp(timestamp):
    """
    Parses a timestamp string and return the year.

    Paramenters
    ----------
    timestamp: A string containing data about when the survey was uploaded

    Returns
    ----------
    year: int

    """

    x = r"([1-9]+)"
    if "year" in timestamp:
        i = int(re.findall(x,timestamp)[0])
        currentTimeDate = datetime.now() - relativedelta(years=i)
        year = currentTimeDate.strftime('%Y')
    elif "month" in timestamp:
        i = int(re.findall(x,timestamp)[0])
        currentTimeDate = datetime.now() - relativedelta(months=i)
        year = currentTimeDate.strftime('%Y')
    elif "day" in timestamp:
        i = int(re.findall(x,timestamp)[0])
        currentTimeDate = datetime.now() - relativedelta(days=i)
        year = currentTimeDate.strftime('%Y')
    elif "hour" in timestamp:
        i = int(re.findall(x,timestamp)[0])
        currentTimeDate = datetime.now() - relativedelta(hours=i)
        year = currentTimeDate.strftime('%Y')
    
    return int(year)



