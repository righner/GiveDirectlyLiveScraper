### Requirements ###
#system
import sys
import os
#import flair

#dask
import dask

#time
from datetime import datetime

#logging
from dask.diagnostics import ProgressBar
pbar = ProgressBar()                
pbar.register() # global registration
from tqdm import tqdm
import logging
logging.basicConfig(filename=os.getcwd()+'/logs/'+str(datetime.now().strftime('%Y-%m-%dT%H-%M-%S'))+'_scraper.log', encoding='utf-8', level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
from timer import Timer



#custom 
from gbq_functions import load_recipient,load_response,get_complete_rids,get_complete_surveys, delete_old_participant_details,try_create_recipient_response_tables,create_aggregate_table
from scraper import scrape_profile, create_payloads
from gender_table import create_gender_table


### Functions ###

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
    logging.info("Sentiment Model loaded")
    try_create_recipient_response_tables()
    #Start with 158000
    rid = start_rid
    #Query a list of profiles that are already complete and thus do not need to be scraped.
    completed_profiles = get_complete_rids()["recipient_id"].values
    logging.info("List of complete profiles loaded")

    completed_surveys = get_complete_surveys()["survey_id"].values
    logging.info("List of complete surveys loaded")

    #logging.info(completed_surveys)
    #logging.info(completed["recipient_id"])
    #completed_profiles = [0]
    #Load profile by profile into the databse
    

    
    for i in tqdm(range(0,number_batches)):
        start = rid
        t = Timer()
        t.start()
        dag = []
        scraped = []
        skipped = 0
        logging.info("\n""Starting to scrape batch "+str(i+1)+"\n")
        for _ in range(0,batch_size):
            if rid not in completed_profiles:
                try:
                    dag.append(dask.delayed(scrape_profile,nout=2)(rid,completed_surveys,sentiment_model))
                except Exception as e:
                    logging.warning("Error at rid "+str(rid))
                    logging.info(e)
                rid += interval
                
                scraped.append(rid)
            else:
                rid += interval
                logging.info(str(rid)+" already completed") 
                skipped += 1
        finish = rid
        #dask.visualize(*dag)
        dask_product = dask.compute(*dag)
        logging.info("All dask tasks completed")
        #logging.info(dask_product)
        recipients_payload, responses_payload, loaded, no_profile, parsing_error, no_updates,unknown_error,empty_response,no_questions  = create_payloads(dask_product)
        if responses_payload != []:
            try:
                logging.info("Finished scraping "+str(len(scraped))+" profiles between rid "+str(start)+" and "+str(finish)+" with interval "+str(interval)+". Skipped "+str(skipped)+" complete profiles \
                \n  Loaded: "+str(loaded)+" \
                \n  No Updates: "+str(no_updates)+" \
                \n  No Profile: "+str(no_profile)+" \
                \n  Parsing Errors: "+str(parsing_error)+" \
                \n  Unknown Errors: "+str(unknown_error)+" \
                \n  Empty response: "+str(empty_response)+" \
                \n  No questions for item: "+str(no_questions))

                #logging.info(responses_payload)
                
                go = load_response(responses_payload)
                if go:
                    load_recipient(recipients_payload)
                else:
                    logging.warning("Loading recipient data cancelled due to error in response payload")

            except Exception as e:
                logging.warning("Error while loading this payload with error:"+"\n     "+str(e))
                logging.warning(responses_payload)
        else:
            logging.info("Finished scraping "+str(len(scraped))+" profiles between rid "+str(start)+" and "+str(finish)+" with interval "+str(interval)+"\n""  Loaded:"+str(loaded)+"\n""  No Updates:"+str(no_updates)+"\n""  No Profile:"+str(no_profile)+"\n""  Parsing Errors:"+str(parsing_error)+"\n""  Unknown Errors:"+str(unknown_error)+"\n""  Skipped "+str(skipped)+" complete profiles")
            logging.info("Nothing to load")
        t.avg_time(batch_size)

    #logging.info(scraped)
    
    delete_old_participant_details()
    
    create_gender_table()

    create_aggregate_table()


if __name__ == "__main__":
    ### Execution ###
    #Standard Values: main(158000,10,62,100)
    #Please get you API key from gender-api.com
    total = Timer()
    total.start()
    main(start_rid=158000,interval=10,number_batches=62,batch_size=100)
    logging.info(total.stop())
    #logging.info(df)
    #sample.to_csv(r'C:\Users\Rainer\Desktop\GiveDirectlyScrape.csv', index = None, header=True)