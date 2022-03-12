# Analysis of Cash transfer recipient survey data. 
## Context
Since about 10 years, an NGO called GiveDirectly is providing unconditional cash transfers to extremely poor individuals, mostly in Easter Africa (Kenya/Uganda). Part of their philosophy is to present themselves as an experiment to proof that unconditional cash transfers are vastly more efficient than most other forms of aid in most contexts (i.e. whenever it is possible and a market exists locally where ppl. can spend the money).
To create transparency about their efficiency, their project is constantly been studied by economists using randomized controlled trials (RCTs). But in addition they also provide raw survey data from recipients on their [website](live.givedirectly.org), where respondents answer question like "How did you spend the money?" and "How did it change your life?".

![Profile](https://user-images.githubusercontent.com/31634583/152777651-4aa12741-c67b-468d-b58f-af093e2dfa6f.png)
## The Problem
The survey data can only be viewed profile by profile. However, it would be much more interesting to see aggregate data, and to filter it by certain parameters such as age or gender.
For another project, I contacted GiveDirectly a few years back if I could get a snapshot of the database. They said no, but they were ok with me scraping the website.
Further, some data point, such as the participants gender, is not given on the website. This data needs to be generated after the extraction.

## The Plan is simple
1. Scrape the data from the wesbite
2. Load it into a database
3. Create a gender classification table
4. Aggregate and clean the Data in a single table 
5. Load the aggregate table into Streamlit Cloud and make it available as a dashboard with wordcloud and sentiment data.

## Pipeline
![Capstone ETLv3](https://user-images.githubusercontent.com/31634583/154586004-99e41adf-76b6-4512-a36b-4f9d99d7aec7.jpg)



## Database ERM
![ERM](https://user-images.githubusercontent.com/31634583/154585972-e5a4ca95-f91f-4fdf-ba26-a790382b1442.png)

## Streamlit output

![image](https://user-images.githubusercontent.com/31634583/154945007-98d84a78-8547-4a85-a027-e253a8c9d5c9.png)


## Current status
The webscraper is complete and uploading to GBQ as well as streamlit was tested. Completed profiles are skipped. Profile are scraped in parallel using dask and data is loaded into BQ in batches which size can be set in the main funtion. Currently 100. This means the scraper scrapes 100 sites at once, and then loads them to BQ, and then goes on.
Incomplete profiles and already loaded questions in incomplete profiles are skipped. 

## Next steps
Schedule and dockerize.

## Questions
- What is the best way to parallelise my scraper? Now Dask.
- How do I generate the batch and what would be the ideal size? Do I compress the batch file and if yes, what format do I use? 
- Which tool should I use for orchestration/scheduling?
