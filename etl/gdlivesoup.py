from bs4 import BeautifulSoup
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        # logging.FileHandler(filename=os.getcwd()+'/logs/'+str(datetime.now().strftime('%Y-%m-%dT%H-%M-%S'))+'_scraper.log'),
        logging.StreamHandler()
    ]
)

from datetime import datetime
from dateutil.relativedelta import relativedelta
import regex as re
import hashlib



class Profile:
    def __init__(self,source, rid, parser = 'lxml', completed_surveys = [], **kwargs):
        self.rid = rid
        self.completed_surveys = completed_surveys
        self.source = source
        self.parser = parser
        self.surveys = []
        self.survey_list = list(set(re.findall(r"payment_\d{1,2}",source )))
        self.survey_list.append("enrollment")
        self.number_surveys = len(self.survey_list)
        self.kwargs = kwargs

        #Details Meta Data
        self.no_age = 0
        self.no_country = 0
        self.no_occupation = 0

        #Survey Meta
        self.scraped_list = []
        self.skipped_list = []
        self.no_questions_list = []
        self.time_parsing_error_list = []
        self.unknown_survey_error_list = []
        self.scrape_count = 0
        self.skip_count = 0
        self.no_updates = False
        self.unknown_empty = False
        self.time_parsing_error = 0
        self.no_questions = 0
        self.unknow_survey_error = 0

    def get_recipient_details(self):
        details = Details(self.source,self.parser,rid=self.rid,**self.kwargs)
        self.details_json = details.get_recipient_details()

        self.details = details

        return self.details_json

    ##### Survey ####
    def get_surveys(self):
        for survey_name in self.survey_list:
            survey_id = self.set_survey_id(survey_name) #create a survey ID in the for rid_X
            if survey_id not in self.completed_surveys: #Skip already loaded payment surveys

                    survey = Survey(self.source,self.parser,rid=self.rid,survey=survey_name,**self.kwargs)
                    survey_json = survey.get_survey_json()
                    if survey_json:
                        self.surveys = [*self.surveys,*survey_json]  #Merge list returned by get_survey_josn into the responses list. The '*' operator unpacks all items from a list.
                        self.scraped_list.append(survey_name)
                        self.scrape_count += 1
                    else:
                        if survey.no_questions:
                            self.no_questions_list.append(survey_name)
                            self.no_questions += 1
                            pass
                        elif survey.time_parsing_error:
                            self.time_parsing_error_list.append(survey_name)
                            self.time_parsing_error += 1
                            pass
                        else:
                            self.unknown_survey_error_list.append(survey_name)
                            self.unknow_survey_error += 1
                            pass
            else:
                logging.info("     Skipped survey "+survey_id)
                self.skipped_list.append(survey_name)
                self.skip_count += 1

        
            
        if self.skip_count != self.number_surveys: #If not all surveys in profile were skipped, then return survey.
            logging.info("Finished scraping Profile "+str(self.rid)+" \
                \n  #Details:  \
                \n  No Age: "+str(self.details.no_age)+" \
                \n  No Country: "+str(self.details.no_country)+" \
                \n  No Occupation: "+str(self.details.no_occupation)+" \
                \n  \
                \n  #Survey Details:  \
                \n  Number surveys: "+str(self.number_surveys)+": "+str(self.survey_list)+" \
                \n  Scraped surveys: "+str(self.scrape_count)+": "+str(self.scraped_list)+" \
                \n  Skipped surveys: "+str(self.skip_count)+": "+str(self.skipped_list)+" \
                \n  Time parsing errors: "+str(self.time_parsing_error)+": "+str(self.time_parsing_error_list)+" \
                \n  Survey errors: "+str(self.unknow_survey_error)+": "+str(self.unknown_survey_error_list)+" \
                \n  Surveys without questions : "+str(self.no_questions)+": "+str(self.no_questions_list))
            return self.surveys
        elif self.skip_count == self.number_surveys: #If all surveys were skipped, tag as "No Updates"
            logging.info("No Updates for rid "+str(self.rid))
            self.no_updates = True
            return None
        elif self.surveys == []: #This should not happen...
            logging.warning("Empty response array returned for rid "+str(self.rid))
            self.unknown_empty = True
            return None

    def set_survey_id(self,survey):
        if survey == 'enrollment':
            return str(self.rid)+"_"+str(0)
        else:
            return str(self.rid)+"_"+re.findall(r"([1-9]+)",survey)[0]


class Details(BeautifulSoup):
    def __init__(self,*args,**kwargs):
        self.rid = kwargs.pop("rid")
        super().__init__(*args,**kwargs)

        #Meta Data
        self.no_age = False
        self.no_country = False
        self.no_occupation = False
        self.complete = False

    ##### Recipient Details #####

    def get_name(self):
        self.name = self.find("div", class_="card-name card-name-profile").text.strip()
        return self.name

    def get_age(self):
        try:
            self.age = self.find("div", class_="fact fact-age").find("div", class_="fact-content").text.strip()
        except AttributeError:
            self.no_age = True
            logging.info("     No age for profile "+str(self.rid))
            self.age= None
        return self.age
    
    def get_country(self):
        try:
            self.country = self.find("div", class_="fact fact-country").find("div", class_="fact-content").text.strip()
        except AttributeError:
            self.no_country = True
            logging.info("     No country for profile "+str(self.rid))
            self.country = None
        return self.country

    def get_occupation(self):
        try:
            self.occupation = self.find("div", class_="fact fact-occupation").find("div", class_="fact-content").text.strip()                    
        except AttributeError:
            self.no_occupation = True
            logging.info("     No occupation for profile "+str(self.rid))
            self.occupation = None
        return self.occupation

    def get_campaign(self):
        self.campaign = self.find("div", class_="fact fact-project").find("div", class_="fact-content").text.strip()
        return self.campaign
        
    def is_complete(self):
        self.complete = (self.find_all("div", class_="no-updates-message") != [])
        return self.complete

    def get_recipient_details(self):
        return [{"recipient_id":self.rid,
        "name":self.get_name(),
        "age":self.get_age(),
        "country":self.get_country(),
        "occupation":self.get_occupation(),
        "completed":self.is_complete(),
        "campaign":self.get_campaign(),
        "last_updated":datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
        ]


class Survey(BeautifulSoup):
    def __init__(self,*args,**kwargs):
        self.survey = kwargs.pop("survey")
        self.rid = kwargs.pop("rid")
        super().__init__(*args,**kwargs)
        self.amount_str = self.get_survey_amount_string()
        self.payment = self.get_payment_number()
        self.questions = self.get_survey_questions()
        self.answers = self.get_survey_answers()
        self.amount = self.get_survey_dollar_amount()
        self.local_amount = self.get_survey_local_amount()
        self.year = self.get_survey_year()
        self.survey_json = []

        #Meta Data
        self.no_questions = False
        self.time_parsing_error = False            

        
    def get_payment_number(self):
        if self.survey == "enrollment": #Set payment code to 0 if enrollment
            return 0 
        else:
            return int(re.findall(r"([1-9]+)",self.survey)[0]) 

    def get_survey_answers(self):
        return self.get_survey_item("survey-answer")
        #logging.info(responses)

    def get_survey_questions(self):
        return self.get_survey_item("survey-prompt")
        #logging.info(questions)

    def get_survey_amount_string(self):
        return self.get_survey_item("transfer-amount-content")

    def get_survey_dollar_amount(self):        
        if self.amount_str == []:
            return None
        else:
            try:        
                return re.findall(r"\$([1-9]+)",self.amount_str[0])[0] #Find all values in format $X 
            except Exception:
                logging.error("Dollar amount missing for rid "+str(self.rid)+"\n"+str(self.amount_str))
                return None

    def get_survey_local_amount(self):        
        if self.amount_str == []:
            return None
        else:
            try:        
                return re.findall(r"(\d+).*",self.amount_str[0])[0] #Return only the number at the beginning of the string (?m)^(\d+).*
            except Exception:
                logging.error("Local amount missing for rid "+str(self.rid)+"\n"+str(self.amount_str))
                return None

     #return number at the end of of payment_X as payment code.
    def get_survey_year(self):
        #print(self.get_survey_item("phase-time")[0])
        self.timestamp = self.get_survey_item("phase-time")[0]
        year = self.parse_timestamp()
        if year:
            return year
        else:
            return None 


    def parse_timestamp(self):
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
        try:
            if "year" in self.timestamp:
                i = int(re.findall(x,self.timestamp)[0])
                currentTimeDate = datetime.now() - relativedelta(years=i)
                year = currentTimeDate.strftime('%Y')
            elif "month" in self.timestamp:
                i = int(re.findall(x,self.timestamp)[0])
                currentTimeDate = datetime.now() - relativedelta(months=i)
                year = currentTimeDate.strftime('%Y')
            elif "day" in self.timestamp:
                i = int(re.findall(x,self.timestamp)[0])
                currentTimeDate = datetime.now() - relativedelta(days=i)
                year = currentTimeDate.strftime('%Y')
            elif "hour" in self.timestamp:
                i = int(re.findall(x,self.timestamp)[0])
                currentTimeDate = datetime.now() - relativedelta(hours=i)
                year = currentTimeDate.strftime('%Y')
            
            return int(year)
        except UnboundLocalError:
            logging.fatal("     Unknown time format at rid "+str(self.rid))
            self.time_parsing_error = True
            return None

    def get_survey_item(self,html_class):
        """
        Takes a BeautifulSoup and extracts all class items from a specific container. 

        Parameters
        ----------
        soup : BeautifulSoup
            A BeautifulSoup file containing a GD-Live profile..
        container_name : str
            A string indicating which survey to load. These are either 'enrollment' or 'payment_X'.
        html_class : str
            A string indicating which part of the survey to load. These are either 'survey_response','transfer-amount-content', 'phase-time', or 'survey-prompt'.

        Returns
        -------
        Container item(s): list
            Returns a list containing all items of a certain class in a container.

        """

        container = self.find("div", {"id" : re.compile(self.survey)})
        return [item.text.strip() for item in container.find_all("div", class_=html_class)]

    def get_survey_json(self):
        if self.year:
            if len(self.questions) > 0:        
                for (response, question) in zip(self.answers,self.questions):
                    response_id = hashlib.md5(bytes(str(self.rid)+self.survey+response, 'utf-8')).hexdigest()
                    #sentence = flair.data.Sentence(response)
                    #sentiment_model.predict(sentence)
                    #logging.info(sentence)
                    self.survey_json.append(
                        {"response_id":response_id,
                        "recipient_id":self.rid,
                        "year":self.year,
                        "payment":self.payment,
                        "usdollar":self.amount,
                        "localfx":self.local_amount,
                        "question":question,           
                        "response":response})
                try:
                    #logging.info(responses_list)
                    return self.survey_json
                except Exception:
                    logging.info("     Unknow survey error for "+self.survey+" in "+str(self.rid))
            else:
                logging.info("     Likely no survey questions asked for "+self.survey+" in "+str(self.rid))
                self.no_questions = True
                return None #Tag as empty survey
        else:
            return None

    #### Survey Data ####

################## Testing Ground ######################        
with open("html/200110",encoding='utf8') as f:
    source = f.read()
    profile = Profile(source,rid=200110)
    # survey = Survey(source,survey='payment_1',rid=1)
    # print(survey.get_survey_item("survey-answer"))

    print(profile.get_recipient_details())
    print(profile.get_surveys())

