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

def main(start_rid=158000,interval=10,number_batches=62,batch_size=100,refresh_gender=True): #Standard samples about 10% of the platform, i.e. every 10th profile until ID 220000
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
    """

    #1 Create tables      
    gbq_functions.try_create_recipient_response_tables() #create tables, if not existing

    #2 Scrape data
    scraper.main(start_rid,interval,number_batches,batch_size)
    
    #3 delete outdated recipient data
    gbq_functions.delete_old_participant_details() 
    
    #4 refresh the gender table
    if refresh_gender:
        gender_table.main()

    #5 create the aggregate table
    gbq_functions.create_aggregate_table()


if __name__ == "__main__":
    from sys import argv
    if len(argv) == 6:
        total = Timer()
        total.start()
        main(start_rid=argv[1],interval=int(argv[2]),number_batches=int(argv[3]),batch_size=int(argv[4]),refresh_gender=eval(argv[5]))
        logging.info(total.stop())
    elif len(argv) == 1:
        total = Timer()
        total.start()
        logging.info("Running with default values: start_rid=158000,interval=10,number_batches=62,batch_size=100")
        main()
        logging.info(total.stop())
    else:
        logging.error("Incorrect number of arguments passed. Should be 4: start_rid,interval ,number_batches ,batch_size")
