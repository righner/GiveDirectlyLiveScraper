from bs4 import BeautifulSoup
from gdlivesoup import Profile
import dask
import os
import requests
from math import ceil
from datetime import datetime
from gbq_functions import load_recipient,load_response,get_complete_rids,get_complete_surveys

# from os import listdir
# from os.path import isfile, join

###### Logging #####
from timer import Timer
from tqdm import tqdm
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.FileHandler(filename=os.getcwd()+'/logs/'+str(datetime.now().strftime('%Y-%m-%dT%H-%M-%S'))+'_scraper.log'),
        logging.StreamHandler()
    ]
)

from dask.diagnostics import ProgressBar
pbar = ProgressBar()                
pbar.register() # global registration 


def main(start_rid,final_rid,interval = 10,parser = 'lxml',batch_size = 100,no_dryrun = True, only_update = True, from_files = True,file_directory = "html"):
    total_time = Timer
    total_time.start()
    #Query a list of profiles that are already complete and thus do not need to be scraped.
    if only_update:
        completed_profiles = get_complete_rids()["recipient_id"].values #Returns the IDs of completed profiles as a numpy array.
        logging.info("List of complete profiles loaded")

        completed_surveys = get_complete_surveys()["survey_id"].values #Returns the IDs of completed survey as a numpy array.
        logging.info("List of complete surveys loaded")

    else:
        completed_profiles = []
        completed_surveys = []

    number_batches = ceil((final_rid-start_rid)/interval/batch_size)
    start = start_rid
    for i in tqdm(range(0,number_batches)):
        batch_timer = Timer()
        batch_timer.start()

        logging.info("Parsing batch "+str(i)+" ...")        

        batch = Batch(start,final_rid,interval,parser,completed_profiles,completed_surveys,batch_size,from_files,file_directory)
        batch.create_payloads()


        if no_dryrun: #if this is not a scraper test, then upload the data to the database.
            if len(batch.responses_payload) > 0:
                try:
                        go = load_response(batch.responses_payload)
                        if go: #if upload of responses successfull, then upload corresponding recipient details.
                            load_recipient(batch.recipients_payload)
                        else:
                            logging.warning("Loading recipient data cancelled due to error in this response payload:")
                            logging.warning(batch.responses_payload)

                except Exception as e:
                    logging.warning("Error while loading this payload: \n     "+str(e))
                    logging.warning(batch.responses_payload)
            else:
                logging.info("Batch precessing returned empty payloads: Nothing was loaded")
        else:
            logging.info("Dryrun finished. Printing Batch: \
                \n \
                \n ############### Recipient Details ###############: \
                \n "+str(batch.recipients_payload)+" \
                \n \
                \n ############### Survey Data ###############: \
                \n "+str(batch.responses_payload))

        start = (batch.finish + interval)
        batch_timer.stop()
    
    total_time.avg_time(number_batches)



class Batch:
    def __init__(self,start_rid,final_rid,interval = 10,parser = 'lxml',completed_profiles = [],completed_surveys = [],batch_size = 100,from_files = True,file_directory = "html"):
        ### Parameters ###
        self.start_rid = start_rid
        self.final_rid = final_rid
        self.interval = interval
        self.batch_size = batch_size
        self.parser = parser
        self.completed_profiles = completed_profiles
        self.completed_surveys = completed_surveys
        self.from_files = from_files
        self.file_directory = file_directory
        
        ### Payloads ####
        self.recipients_payload = []
        self.responses_payload = []

        ### Meta Data ###
        self.added_rids = []
        self.skipped_rids = []
        self.added = 0
        self.skipped = 0
        self.scraped = 0
        self.parsed = 0
        self.no_profile_list = []
        self.no_profile = 0
        self.request_error_list = []
        self.request_error = 0
        self.no_file_list = []
        self.no_file = 0
        self.parsing_error_list = []
        self.parsing_error = 0
        self.unknown_error_list = []
        self.unknown_error = 0

        #Survey Meta Data
        self.parsed_surveys = 0
        self.skipped_surveys = 0
        self.no_updates = 0
        self.unknown_empty_list = []
        self.unknown_empty = 0
        self.time_parsing_error_list = []
        self.time_parsing_error = 0
        self.no_questions_list = []
        self.no_questions = 0
        self.unknow_survey_error_list = []
        self.unknow_survey_error = 0

        #Dask        
        self.__creating_dask_dag()

    
    def create_payloads(self):
        "Returns  both the recipient and the response payload as json files"
        
        dask_product = dask.compute(self.dag)[0]
        for rid,load in dask_product: #Sort each profile according to contents
            if load:
                try:
                    self.recipients_payload = [*self.recipients_payload,*load[0]]
                    self.responses_payload = [*self.responses_payload,*load[1]]
                    self.parsed += 1
                except Exception as e:
                    logging.warning("   Error while parsing the load of "+str(rid)+":"+str(load)+"\n     "+str(e))
                    self.parsing_error_list.append(rid)
                    self.parsing_error += 1  
            else:
                pass
        
        logging.info("Finished parsing "+str(self.added)+" profiles between rid "+str(self.start_rid)+" and "+str(self.finish)+" with interval "+str(self.interval)+". \
                \n  Scraped: "+str(self.scraped)+" \
                \n  Skipped: "+str(self.skipped)+" \
                \n  Parsed: "+str(self.parsed)+" \
                \n  No Updates: "+str(self.no_updates)+" \
                \n  No Profile: "+str(self.no_profile)+": "+str(self.no_profile_list)+" \
                \n  Parsing Errors: "+str(self.parsing_error)+": "+str(self.parsing_error_list)+" \
                \n  Request Errors: "+str(self.request_error)+": "+str(self.request_error_list)+" \
                \n  No file: "+str(self.no_file)+": "+str(self.no_file_list)+" \
                \n  Unknown Errors: "+str(self.unknown_error)+": "+str(self.unknown_error_list)+" \
                \n  Empty response: "+str(self.unknown_empty)+": "+str(self.unknown_empty_list)+" \
                \n  \
                \n  #Survey level stats: \
                \n  Parsed surveys: "+str(self.parsed_surveys)+" \
                \n  Skipped surveys: "+str(self.skipped_surveys)+" \
                \n  Time Parsing Errors: "+str(self.time_parsing_error)+": "+str(self.time_parsing_error_list)+" \
                \n  Unknow survey errors: "+str(self.unknow_survey_error)+": "+str(self.unknow_survey_error_list)+" \
                \n  Surveys without questions: "+str(self.no_questions)+": "+str(self.no_questions_list))


    def __creating_dask_dag(self):
        self.dag = []
        rid = self.start_rid      
        while rid < (self.start_rid + self.batch_size * self.interval) and rid < self.final_rid:  
            if rid not in self.completed_profiles: #Skip profiles marked as completed

                self.dag.append(dask.delayed(self.__scrape_profile,nout=2)(rid)) #Add scraper to dask dask dag specifying two expected outputs.

                rid += self.interval
                
                self.added_rids.append(rid)
            else:
                rid += self.interval
                self.skipped_rids.append(rid)

        self.finish = rid
        self.added = len(self.added_rids)
        self.skipped = len(self.skipped_rids)
        logging.info("This batch will scrape "+str(self.added)+" profiles. "+str(self.skipped)+" complete profiles will be skipped") 

    def __scrape_profile(self,rid):       

        if self.from_files:
            profile = self.__get_profile_from_file(rid)
        else:
            profile = self.__get_profile_from_web(rid)
        try:
            if profile: #When no profile with this rid exists or no file was found, profile will be None
                
                recipient = dask.delayed(profile.get_recipient_details)()

                responses = dask.delayed(profile.get_surveys)()    

                dask_product = dask.compute(recipient,responses) #Execute processing of recipient data and responses in parallel. Such nested delaying is NOT recommended in the dask documentation, but turned out to be faster in this case.

                self.parsed_surveys += profile.scrape_count
                self.skipped_surveys += profile.skip_count

                if profile.time_parsing_error > 0:
                    self.time_parsing_error += profile.time_parsing_error
                    self.time_parsing_error_list.append(rid)

                if profile.no_questions > 0:
                    self.no_questions_list.append(rid)
                    self.no_questions += profile.no_questions

                if profile.unknow_survey_error > 0:
                    self.unknow_survey_error_list.append(rid)
                    self.unknow_survey_error += profile.unknow_survey_error

                if profile.no_updates:
                    self.no_updates += 1
                    return (rid, None )
                elif profile.unknown_empty:
                    self.unknown_empty_list.append(rid)
                    self.unknown_empty += 1
                    return (rid, None )
                else:
                    return (rid, dask_product)

            else:
                return (rid, None )

        except Exception as e:
            logging.error("Unknown error at rid "+str(rid)+"\n   "+str(e))
            self.unknown_error_list.append(rid)
            self.unknown_error += 1
            return (rid, None )

   
    def __get_profile_from_file(self,rid):
        path = os.path.join(self.file_directory, str(rid))
        try:
            with open(path,encoding='utf-8') as f:
                source = f.read()
                profile = Profile(source, rid,self.parser,self.completed_surveys)  
                if "Sorry, the recipient you're looking for can't be found" in str(BeautifulSoup(source,'lxml')):
                    logging.info("No Profile for rid "+str(rid))
                    self.no_profile_list.append(rid)
                    self.no_profile += 1
                    return None
                else:
                    logging.info("File loaded for rid "+str(rid))
                    self.scraped += 1
                    return profile
        except FileNotFoundError:
            self.no_file_list.append(rid)
            self.no_file += 1
            logging.info("File could not be found for rid "+str(rid))
            return None
    
    def __get_profile_from_web(self,rid):
        """
        Requests the data from profile page and return it both as BeautifulSoup and as string. 
        
        Parameters
        ----------
        recipient_url : str
            A string containing the url of the GD-Live profile.
        rid: int
            A recipient ID.

        Returns
        -------
        profile: BeautifulSoup
            Returns a BeautifulSoup containing the profile data.
        Source: str
            HTML of profile site.

        """
        try:
            recipient_url = "https://live.givedirectly.org/newsfeed/a7de23c0-39af-4af3-9671-13dc38a85e26/"+str(rid)+"?context=newsfeed"
            source = requests.get(recipient_url).text
            profile = Profile(source, rid,self.parser,self.completed_surveys)
            if "Sorry, the recipient you're looking for can't be found" in str(BeautifulSoup(source,'lxml')):
                logging.info("No Profile for rid "+str(rid))
                self.no_profile_list.append(rid)
                self.no_profile += 1
                return None
            else:
                logging.info("Data extracted from recipient "+str(rid))
                self.scraped += 1
                return profile
        except Exception as e:
            self.request_error_list.append(rid)
            self.request_error += 1
            logging.error("Failure resquesting from rid "+str(rid)+ "\n     "+str(e))
            pass


############### Testing Ground #######################

# batch = Batch(159100,210000,100,completed_profiles=[159140])
# batch.create_payloads()
# print(batch.responses_payload)
# print(batch.recipients_payload)

main(158000,220000,no_dryrun=False, only_update=False)