### Requirements ###
#system
import sys

import logging
#logging.basicConfig(filename=os.getcwd()+'/logs/'+str(datetime.now().strftime('%Y-%m-%dT%H-%M-%S'))+'_scraper.log', encoding='utf-8', level=logging.INFO) #For local logging
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
from timer import Timer

#own modules 
import gbq_functions
import scraper
import gender_table

def main(start_rid=158000,interval=10,number_batches=62,batch_size=100,refresh_gender=True,from_files=True): #Standard samples about 10% of the platform, i.e. every 10th profile until ID 220000
    """
    Executes the scraper 

    Parameters
    ----------
    start_rid : int
        The recipient ID from which to start loading the GD-Live profile. Please not that the minimum is around 158000.
    interval : int
        The interval at which to sample rid's.
    number_batches : int
        The number of batches to load.    
    batch_size: int
        The size of each batch
    refresh_gender: bool
        If True, the gender table will be recreated as well
    from_files: bool
        If True, the scraper will look for html files stored in the "html" folder, otherwise profiles will be scraped directly.
    """

    #1 Create tables      
    gbq_functions.try_create_recipient_response_tables() #create tables, if not existing

    #2 Scrape data
    scraper.main(start_rid,interval,number_batches,batch_size,no_dryrun=True,from_files=from_files)
    
    #3 delete outdated recipient data
    gbq_functions.delete_old_participant_details() 
    
    #4 refresh the gender table
    if refresh_gender: #Skip in case there aren't enough credits left on Namsor
        gender_table.main()

    #5 create the aggregate table
    gbq_functions.create_aggregate_table()


if __name__ == "__main__":
    from sys import argv
    if len(argv) == 7:
        total = Timer()
        total.start()
        main(start_rid=argv[1],interval=int(argv[2]),number_batches=int(argv[3]),batch_size=int(argv[4]),refresh_gender=eval(argv[5]),from_files=eval(argv[6]))
        logging.info(total.stop())
    elif len(argv) == 1:
        total = Timer()
        total.start()
        logging.info("Running with default values: start_rid=158000,interval=10,number_batches=62,batch_size=100,refresh_gender=True,from_files=True")
        main()
        logging.info(total.stop())
    else:
        logging.error("Incorrect number of arguments passed. Should be 6: start_rid,interval ,number_batches ,batch_size,refesh_gender,from_files")
