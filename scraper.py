import requests
from bs4 import BeautifulSoup
import regex as re
import dask
import flair

from timer import Timer
from gbq_functions import load_recipient,load_response,get_complete_rids,get_complete_surveys, delete_old_participant_details

#Pofiling cmd: kernprof -l -v "G:\OneDrive\Documents\Projects\GDLive Scraper\Script.py"
 

### Functions ###
#@profile
def main(start_rid, interval, number_batches, batch_size):
    """
    Loads profiles from the GDLive Website into a Database (i.e. BigQuery).

    Parameters
    ----------
    start_rid : int
        The recipient ID from which to start loading the GD-Live profile. Please not that the minimum is around 158000.
    interval : int
        The interval at which to sample rid's.
    number : int
        The amount of profiles to load.    

    """

            
    sentiment_model = 0 # flair.models.TextClassifier.load('en-sentiment')
    print("Sentiment Model loaded")

    #Start with 158000
    rid = start_rid
    #Query a list of profiles that are already complete and thus do not need to be scraped.
    completed_profiles = get_complete_rids()["recipient_id"].values
    print("List of complete profiles loaded")

    completed_surveys = get_complete_surveys()["survey_id"].values
    print("List of complete surveys loaded")

    #print(completed_surveys)
    #print(completed["recipient_id"])
    #completed_profiles = [0]
    #Load profile by profile into the databse
    scraped = []

    
    for _ in range(0,number_batches):
        start = rid
        t = Timer()
        t.start()
        dag = []
        skipped = 0

        for _ in range(0,batch_size):
            if rid not in completed_profiles:
                try:
                    dag.append(dask.delayed(scrape_profile,nout=2)(rid,completed_surveys,sentiment_model))
                except Exception as e:
                    print("Error at rid "+str(rid))
                    print(e)
                rid += interval
                
                scraped.append(rid)
            else:
                rid += interval
                print(str(rid)+" already completed") 
                skipped += 1
        finish = rid
        dask.visualize(*dag)
        recipients_payload, responses_payload, loaded, empty, error = create_payloads(dask.compute(*dag))
        try:
            print("Finished scraping "+str(len(scraped))+" profiles between rid "+str(start)+" and "+str(finish)+" with interval "+str(interval)+"\n""  Loaded:"+str(loaded)+"\n""  Empty:"+str(empty)+"\n""  Parsin Errors:"+str(error)+"\n""  Skipped "+str(skipped)+" complete profiles")

            load_response(responses_payload)
            load_recipient(recipients_payload)

        except Exception as e:
            print( "Error while loading this payload:", e)
            print(responses_payload)
        t.avg_time(batch_size)

    print(scraped)
    delete_old_participant_details(scraped)
    

def create_payloads(dask_output):
    recipients_payload = []
    responses_payload = []
    loaded = 0
    empty = 0
    error = 0
    for load in dask_output:
        if load != "Profile does not exist": 
            try:
                recipients_payload.append(load[0][0])
                responses_payload.append(load[1][0])
                #print(load[1][0])
                loaded += 1
            except:
                print("Error while parsing this load:"+str(load))
                error += 1
        else:
            empty += 1
    return recipients_payload, responses_payload, loaded, empty, error



def scrape_profile(rid,completed_surveys,sentiment_model):
    
    try:
        
        
        url = "https://live.givedirectly.org/newsfeed/a7de23c0-39af-4af3-9671-13dc38a85e26/"+str(rid)+"?context=newsfeed"

        profile, source = load_recipient_profile(url,rid)

        recipient = dask.delayed(get_recipient_details)(profile,rid)

        responses = dask.delayed(get_recipient_surveys)(profile,rid,source,completed_surveys,sentiment_model)
        
        return dask.compute(recipient,responses)

    except AttributeError as e: 
          
        try:
            friendly_error =  friendly_error(profile)
            if friendly_error == "Sorry, the recipient you're looking for can't be found":
                return "Profile does not exist"
            else:
                print("Enountered unknown friendly; ",friendly_error)
        except:                  
            print("Unknown Attribute error at rid "+str(rid)+"\n""   ",e)
            pass

def friendly_error(profile):
    friendly_error = profile.find("h1", class_="friendly-error").text.strip()
    #print(friendly_error)
    return friendly_error
    

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
    enrollment_id = str(rid)+"_"+str(0)
    if enrollment_id not in completed_surveys:
        try:
            responses.append(get_survey_jsons(rid,profile,"enrollment",sentiment_model))
        except AttributeError:
            print("     No survey class 'enrollment' found in rid "+str(rid))
    else:
        print("     Skipped survey "+enrollment_id)

    payments = list(set(re.findall(r"payment_\d{1,2}",source )))
    for payment in payments:
        payment_id = str(rid)+"_"+re.findall(r"([1-9]+)",payment)[0]
        if payment_id not in completed_surveys:
                responses.append(get_survey_jsons(rid,profile,payment,sentiment_model))
                #print("     Payment "+payment+" extracted")
        else:
            print("     Skipped survey "+payment_id)
        

       
    return responses


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
        print("Data extracted from recipient "+str(rid))
        return BeautifulSoup(source, "lxml"), source
    except:
        print("Failure resquesting from rid "+str(rid))
        
    


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
        print("     No age for profile "+str(rid))
        age= None
    try:
        country = profile.find("div", class_="fact fact-country").find("div", class_="fact-content").text.strip()
    except AttributeError:
        print("     No country for profile "+str(rid))
        country = None
    try:
        occupation = profile.find("div", class_="fact fact-occupation").find("div", class_="fact-content").text.strip()        
    except AttributeError:
        print("     No occupation for profile "+str(rid))
        occupation = None
    campaign = profile.find("div", class_="fact fact-project").find("div", class_="fact-content").text.strip()
    complete = (profile.find_all("div", class_="no-updates-message") != [])
    
    from datetime import datetime
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
    import hashlib
    
    responses = get_profile_item(profile, survey,"survey-answer")
    questions = get_profile_item(profile, survey,"survey-prompt")

    amount_str = get_profile_item(profile, survey,"transfer-amount-content")

    if amount_str == []:
        amount = None
        local_amount = None
    else:
        
        amount = re.findall(r"\$([1-9]+)",amount_str[0])[0]
        local_amount = re.findall(r"(?m)^(\d+).*",amount_str[0])[0]

    if survey == "enrollment":
        payment = 0
    else:
        payment = int(re.findall(r"([1-9]+)",survey)[0])

    timestamp = get_profile_item(profile, survey,"phase-time")[0]
    try:
        year = parse_timestamp(timestamp)
    except UnboundLocalError:
        print("     Unknown time format at rid "+str(rid))
        exit()

    


    responses_list = []
    for (response, question) in zip(responses,questions):
        response_id = hashlib.md5(bytes(str(rid)+survey+response, 'utf-8')).hexdigest()
        #sentence = flair.data.Sentence(response)
        #sentiment_model.predict(sentence)
        #print(sentence)
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
        return responses_list[0]
    except IndexError:
        print("     Likely no survey questions asked for "+survey+" in "+str(rid))

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
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
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


### Execution ###
#Standard Values: main(158000,10,62,100)
#Please get you API key from gender-api.com
total = Timer()
total.start()
main(158000,10,62,100)
total.stop()
#print(df)
#sample.to_csv(r'C:\Users\Rainer\Desktop\GiveDirectlyScrape.csv', index = None, header=True)
