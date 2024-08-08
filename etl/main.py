### Requirements ###
#system
from datetime import datetime
import os
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.FileHandler(filename=os.getcwd()+'/logs/'+str(datetime.now().strftime('%Y-%m-%dT%H-%M-%S'))+'_scraper.log'),
        logging.StreamHandler()
    ]
)
from timer import Timer

#own modules 
import gbq_functions
import update
import gender_table

def main(start_rid =158000,final_rid=220000,interval = 10,parser = 'lxml',batch_size = 100,no_dryrun = True, only_update = True, from_files = True,file_directory = "html",refresh_gender=True): #Standard samples about 10% of the platform, i.e. every 10th profile until ID 220000
    """
    Updates the Database
    """

    #1 Create tables      
    gbq_functions.try_create_recipient_response_tables() #create tables, if not existing

    #3 Download files      
    if from_files:
        file_name = datetime.now().strftime("%Y%m")+".tar.gz"
        gbq_functions.download_unpack_htmls("gdliveproject_htmls",file_name,file_directory)

    #2 Scrape data
    update.main(start_rid,final_rid,interval,parser ,batch_size,no_dryrun, only_update, from_files,file_directory)

    #3 Remove files      
    os.rmdir(file_directory)
    
    #5 delete outdated recipient data
    gbq_functions.delete_old_participant_details() 
    
    #6 refresh the gender table
    if refresh_gender: #Skip in case there aren't enough credits left on Namsor
        gender_table.main()

    #7 create the aggregate table
    gbq_functions.create_aggregate_table()


if __name__ == "__main__":
    from sys import argv
    if len(argv) == 11:
        total = Timer()
        total.start()
        main(start_rid =argv[1],final_rid=argv[2],interval = argv[3],parser = argv[4],batch_size = argv[5],no_dryrun = eval(argv[6]), only_update = eval(argv[7]), from_files = eval(argv[8]),file_directory = argv[9],refresh_gender=eval(argv[10]))
        logging.info(total.stop())
    elif len(argv) == 1:
        total = Timer()
        total.start()
        logging.info("Running with default values: start_rid =158000,final_rid=220000,interval = 10,parser = 'lxml',batch_size = 100,no_dryrun = True, only_update = True, from_files = True,file_directory = 'html',refresh_gender=True")
        main()
        logging.info(total.stop())
    else:
        logging.error("Incorrect number of arguments passed. Should be none or all (10): start_rid,final_rid,interval,parser ,batch_size,no_dryrun, only_update, from_files,file_directory,refresh_gender")
