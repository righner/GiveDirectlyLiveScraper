# Scraping and analysing cash transfer recipient survey data 
## Contents
1. Context
2. The Problem
3. The Idea
4. Pipeline
5. Database ERM
6. The Scraper
	1. Process
	2. Parallelizing using Dask
	3. Scheduling
7. Streamlit Dashboard
8. Speeding up NLTK by almost 30x
9. Caching globally on Streamlit Cloud using pickles
10. Current status
11. Possible next steps/ ideas
12. Current Questions	

## Context
Since about 10 years, an NGO called GiveDirectly is providing unconditional cash transfers to extremely poor individuals, mostly in Easter Africa (Kenya/Uganda). Part of their philosophy is to present themselves as an experiment to proof that unconditional cash transfers are vastly more efficient than most other forms of aid in most contexts (i.e. whenever it is possible and a market exists locally where people can spend the money).
To create transparency about their efficiency, their project is constantly been studied by economists using randomized controlled trials (RCTs). But in addition, they also provide raw survey data from recipients on their [website](live.givedirectly.org), where respondents answer question like "How did you spend the money?" and "How did it change your life?".

![Profile](https://user-images.githubusercontent.com/31634583/152777651-4aa12741-c67b-468d-b58f-af093e2dfa6f.png)
## The Problem
The survey data can only be viewed profile by profile. It would however also be interesting to see some aggregate results, and to filter it by certain parameters such as age, campaign or amount received, but also gender. The latter is difficult since the recipients gender is not explicitly given on the website. It has to be derived indirectly from the name.  

## The Idea
1. Scrape the data from the wesbite
2. Load it into a database
3. Use an API to classify the recipients gender based on their name
4. Aggregate and clean the responses in a single table so they can be filtered and analyzed 
5. Load the aggregate table into Streamlit Cloud and make it available as a dashboard with wordcloud, wordcount, and possibly sentiment data.

## Pipeline
![Pipeline](https://user-images.githubusercontent.com/31634583/162624253-028a8083-5a19-4693-aa28-a1482a2983d0.png) 

## Database ERM
![ERM](https://user-images.githubusercontent.com/31634583/159188828-51b7dd5f-f7f7-4cd9-a6cd-e657d08a68c5.png)

## The Scraper
### Process
1. Using make:
	1. Get the html source from the webpage using the 6-digit recipient-ID found at the end of the profile URL (or skip it if complete and already in database.)
	2. Store the tar.gz containing the htmls in Google Cloud Storage
2. Using BeautifulSoup
	1. Scrape the recipients' info, i.e. Name, Age, Campaign, Complete?, etc.
	2. List and scrape the surveys of the profile (and skip those already in the database).
3. Parse the data into two JSON payloads.
4. Load it into BigQquery

### Parallelization using Dask
To speed up the scraping process, I m using dask to run the processing of each profile in parallel. The scraper currently takes a list of 100 profiles as a batch. If a profile is not already completed and in the database, a task is added to a dag to scrape the profile.

```python
for _ in range(0,batch_size): #Loop profiles within batch
    if rid not in completed_profiles: #Skip profiles marked as completed
        try:
            dag.append(dask.delayed(scrape_profile,nout=2)(rid,completed_surveys)) #Add scraper to dask dag specifying two expected outputs.
        except Exception as e:
            logging.warning("Error creating task at rid "+str(rid))
            logging.info(e)
        rid += interval #set the next rid to be scraped
       
        scraped.append(rid) #Add rid to a list of scraped rids
    else:
        rid += interval #set the next rid to be scraped
        logging.info(str(rid)+" already completed")
        skipped += 1 #Update metadata on skipped profiles
finish = rid #set last recipient id scraped
dask_product = dask.compute(*dag) #compute tasks
```
Even though it is [not recommended in the official documentation](https://docs.dask.org/en/stable/delayed-best-practices.html), I also run a second level of paralellization within the task. Here, I do the processing of the recipient info and that of the survey data in parallel.

```python
recipient = dask.delayed(get_recipient_details)(profile,rid)

responses = dask.delayed(get_recipient_surveys)(profile,rid,source,completed_surveys)

return dask.compute(recipient,responses) #Execute processing of recipient data and responses in parallel. Such nested delaying is NOT recommended (https://docs.dask.org/en/stable/delayed-best-practices.html) in the dask documentation, but turned out to be faster in this case.
```

### Scheduling
The scraper runs on the last day of the month. The reason for this is the pickled analyses on streamlit are names after the month in which they were run. This way, the old cache expires on the first of each month and users will automatically start with a updated dataset.

The problem with the scheduling is that this schedule cannot be implemented in Unix crontab notation. We would need a hack similar to this:

```bash
00 00 27-31 * * [ "$(date +%d -d tomorrow)" = "01" ]
```
While this obviously is not an unsurmountable issue, I still decided to use Airflow instead. Firstly, because I wanted to get experience deploying a it in a cloud setting, and secondly because Airflow has a custom notation for crontab, i.e. the L notation for the last day unit.

```bash
00 00 L * *
```

For now, all Airflow does it do run a single task dag, namely to pull and run the latest image of my dockerized scraper: 
```python
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from textwrap import dedent

with DAG(dag_id="monthly_update",
            description='Run the scraper and transformations on last day of the month',
            start_date=datetime(2022,3,31,0,0),
            schedule_interval="00 00 L * *", # 00:00 on the last day of the month
            catchup=False
) as dag:
    update_db = BashOperator(task_id = "update_db",
                                bash_command= "sudo docker run europe-west1-docker.pkg.dev/gdliveproject/docker/
								full:latest”
                                dag=dag)

    update_db
```
I also opened a port on my Compute Engine Virtual Machine so that I can manage the dag on the webserver.

![Airflow Webserver](https://user-images.githubusercontent.com/31634583/161578993-b7772a28-b0d8-40fe-b77c-3615cf27d327.png)


The entrypoint in the container is a main function, which runs all necessary steps for the update in sequence. It takes as arguments at which rid to start the update, at what interval to sample the profiles - i.e. 10 meaning to sample every 10th profile - how many batches to load, the size of each batch, and finally whether to refresh the gender table, which needs to be handled with care as it uses up limited credits every time it runs. 

```
def main(start_rid=158000,interval=10,number_batches=62,batch_size=100,refresh_gender=True): #Standard samples about 10% of the platform, i.e. every 10th profile until ID 220000


    #1 Create tables      
    gbq_functions.try_create_recipient_response_tables() #create tables, if not existing

    #2 Scrape data
    scraper.main(start_rid,interval,number_batches,batch_size)
   
    #3 delete outdated recipient data
    gbq_functions.delete_old_participant_details()
   
    #4 refresh the gender table
    if refresh_gender: #Skip in case there aren't enough credits left on Namsor
        gender_table.main()

    #5 create the aggregate table
    gbq_functions.create_aggregate_table()
```



## Streamlit Dashboard
I chose Streamlit for my web-app since it is python based, quick to implement and deploy - especially on Streamlit's own servers - and very easy to learn. It certainly has limitations, but for my purposes was more than enough. I am not a web developer, so something that gave me all the data-plotting tools without needing to deal with any JavaScript/HTML/CSS was just right.

The streamlit dashboard guides the user though three steps. 
### Step I: Set a filter
![Filter](https://user-images.githubusercontent.com/31634583/159189268-4fe4ebdc-e8a6-4c03-a140-5dafab79f9e6.png)

### Step II: Create the Wordcloud
![Wordcloud](https://user-images.githubusercontent.com/31634583/159189042-52c00610-c312-447a-a3b3-8b3e16cc55df.png)

### Step III: Count the nouns, verbs and adjectives

![Wordcount](https://user-images.githubusercontent.com/31634583/159189078-d56b8f31-89b8-49ab-ad40-6865ce85b2f5.png)

## Speeding up NLTK by almost 30x
To realize the wordcount, I decided to use nltk as it seemed to have comparatively good performance in terms of accuracy when predicting the grammatical type of word. The classification is done by the pos_tag function, which takes as an input not raw text, but a list of words. Thus, before passing the text to post_tag, it needs to be split word by word. NLTK provides the sent_tokenize and word_tikenize functions for this purpose. The first functions will split the raw text by sentence marker (.,?,!, etc.), creating a list of sentences, while the latter again splits it word by word.

```python
sentences = nltk.sent_tokenize(text) #split by sentence marker (.,?,!)
words = []
for sentence in sentences:
    words = [*words,*nltk.word_tokenize(sentence)] #unpack and merge the two lists 
```
The pos_tag functions then creates a list of word-tag tuples, which can be used to sort out the list by tag.

```python 
tagged = nltk.pos_tag(words)

noun_list = []
verb_list = []
adj_list = []
for (word, tag) in tagged:
    if tag in ['NN','NNP','NNS']: # If the word is a noun
        noun_list.append(word)
    elif tag in ['VB','VBD','VBG','VBN','VBP','VBZ']: # If the word is a verb
        verb_list.append(word)
    elif tag in ['JJR','JJS']: # If the word is an adjective
        adj_list.append(word) 
```



One issue that I dealt with however was the very long time it took for the entire process (up to 30 minutes for the full dataset). This surprised me, since approximately 60k responses in my 10% sample did not sound like a lot to me. 
It turned out that the perpetrator in the case of the nltk package was not the pos_tag funtion as one would think, but the tokenizer funtions, and the sent_tokenize in particular. 

sent_tokenize actually calls the PunktSentenceTokenizer (in version 3.7), which in turn "uses an unsupervised algorithm to build a model for abbreviation words, collocations, and words that start sentences; and then uses that model to find sentence boundaries" (see [the documentation](https://github.com/nltk/nltk/blob/develop/nltk/tokenize/punkt.py)). This is likely done to make the function more flexible in regard to unaccounted sentence markers, but also causes the extra time needed for a normally fast operation, i.e. splitting a sentence into a list of words. Thus, as long as the text is only plain English without any exotic sentence markers, it will be absolutely sufficient to simply split the text by the most common markers, i.e. ".","!" and "?".  

Thus, to speed up the process, I revert to numpy to first replace the sentence markers with spaces, and then simply split the resulting clean string with the str.split operator.  

```python
temp = np.char.replace(text,"."," ") # Use instead of nltks slow sent_tokenize()
temp = np.char.replace(temp,","," ") #remove commas
temp = np.char.replace(temp,"!"," ") #remove exclamations marks
clean_text = np.char.replace(temp,"?"," ") #remove question marks    
words = str.split(str(clean_text)) #split text into single words #FYI using np.char.split here caused an Index Error when trying to split the np array into partitions using np.array_split.
```

This already brought the total processing time down to just under 3 minutes (from 30 minutes before)! But I did not want to stop there. As another performance tweak, I used dask to run the pos_tag function in parallel on parts of the words list. It turned out that 15 partitions resulted in the fastest processing time. In this setup, the entire sample was counted in just over one minute, 30x faster than the approach from the documentation.

```python
tasks = []
split_array = np.array_split(words,15) #A test on the full dataset showed that 15 parallel dask tasks are optimal.
for array in split_array:
    task = delayed(nltk.pos_tag)(array)
    tasks.append(task)        

dask_product = compute(*tasks) #A list of lists containing word-tag tuples
```

## Caching globally on Streamlit Cloud using pickles
Another quick performance hack for better user experiene was to pickle the full dataset as well as the analyses in the container itself, instead of using the st.cache decorator to store data only in the users browser. Downloading the dataset from BigQuery already takes a few seconds, while it happens almost instantly when the dataset is just loaded from a local pickle file. Even larger is the improved performance for the word counts, as the analysis on the full dataset still takes over one minute, even with significantly improved performance. 

These two funtions serve are everything you need to make it happen. They also store any type of data and also bundle multiple files in one pickle. 

```python
def pickle_data(pickle_id,*args):
   
    with open(pickle_id+".pickle", "wb") as f:
        pickle.dump(len(args), f)
        for value in args:
            pickle.dump(value, f)

def read_pickled_data(pickle_id):
    data = []
    with open(pickle_id+".pickle", "rb") as f:
        for _ in range(pickle.load(f)):
            data.append(pickle.load(f))
    return data #list
```
The only other thing needed is a unique ID for each file. The pickle_id for the aggregate dataset is simply based on the month, i.e. 202203_agg_df for March 2022. For the wordclouds and wordcounts, I generate a hash value from the filter settings and then append the month.

```python
month = datetime.now().strftime('%Y%m_')    
filter_hash = hashlib.md5(bytes(str(gender)+str(question)+str(campaign)+str(min_amount)+str(max_amount), 'utf-8')).hexdigest()
filter_id = month+filter_hash
```
I then append "count" or "cloud", depending on the analysis. For the latter, I also need to account for cloud customization settings.
```
...
cloud_id= "cloud_" + filter_id + str(max_word) + str(max_font)+ str(random)
...
count_id = "count_"+filter_id
```


## Current status
### Scraper
The scraper now, unlike before, first downloads all the profiles to be scraped and stores them in a compressed file on Google Cloud Storage. 10 profiles are downloaded in parallel. Completed profiles already stored in BigQuery are skipped
Then, the files are scraped using BeautifulSoup. Profiles are scraped in parallel using dask and data is loaded into BQ in batches which size can be set manually (currently 100). Batching is done to make the scraper more resilient to errors and to make help with debugging  
Incomplete profiles and already loaded surveys in incomplete profiles are skipped.
For now, I am using Google Cloud Build for CI.
The database is updated once a month, scheduled via Airflow.


### Streamlit
Basic features, including WordCloud, wordcount, and filtering are implemented.
Improved caching in the Streamlit Cloud container image instead of browser is implemented.

## Possible next steps/ ideas
### Streamlit
Add session_states and on_update funtions to prevent conflicting filter settings.  
Add sentiment analysis using Flink.
Cache persistently, i.e. on Google Cloud Storage.

### Scraper
Add unit tests


### Cloud Run Deployment on hold
My project, like [many others](https://github.com/streamlit/streamlit/issues/3028), faces issues with Streamlit deployment on Google Cloud Run. Streamlit uses a URL for their health checks that is reserved on GCP, leading to a 404 error shortly after loading the app. 
![image](https://user-images.githubusercontent.com/31634583/160145979-f9e57f34-ba70-448b-9b5b-cd49a71a0f1a.png)

While there are some [hacky workarounds](https://discuss.streamlit.io/t/has-anyone-deployed-to-google-cloud-platform/931/24), I decided to keep hosting my app on Streamlit Cloud, which is perfectly fine. I simply wanted to use hosting it on Cloud Run to get a feeling for CI in a web-dev context.


## Current questions
- What is the best way to parallelize my scraper? Now dask.

