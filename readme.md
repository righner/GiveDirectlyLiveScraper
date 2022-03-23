# Scraping and analysing cash transfer recipient survey data 
## Context
Since about 10 years, an NGO called GiveDirectly is providing unconditional cash transfers to extremely poor individuals, mostly in Easter Africa (Kenya/Uganda). Part of their philosophy is to present themselves as an experiment to proof that unconditional cash transfers are vastly more efficient than most other forms of aid in most contexts (i.e. whenever it is possible and a market exists locally where ppl. can spend the money).
To create transparency about their efficiency, their project is constantly been studied by economists using randomized controlled trials (RCTs). But in addition they also provide raw survey data from recipients on their [website](live.givedirectly.org), where respondents answer question like "How did you spend the money?" and "How did it change your life?".

![Profile](https://user-images.githubusercontent.com/31634583/152777651-4aa12741-c67b-468d-b58f-af093e2dfa6f.png)
## The Problem
The survey data can only be viewed profile by profile. However, it would be much more interesting to see aggregate data, and to filter it by certain parameters such as age or gender.
For another project, I contacted GiveDirectly a few years back if I could get a snapshot of the database. They said no, but they were ok with me scraping the website.
Further, some data point, such as the participants gender, is not given on the website. This data needs to be generated after the extraction.

## The Idea
1. Scrape the data from the wesbite
2. Load it into a database
3. Create a gender classification table
4. Aggregate and clean the data in a single table 
5. Load the aggregate table into Streamlit Cloud and make it available as a dashboard with wordcloud and possibly sentiment data.

## Pipeline
![Pipeline](https://user-images.githubusercontent.com/31634583/159188409-fb204aee-2261-49bc-8e4e-ef16a61d702c.png)


## Database ERM
![ERM](https://user-images.githubusercontent.com/31634583/159188828-51b7dd5f-f7f7-4cd9-a6cd-e657d08a68c5.png)

## Streamlit output
![Filter](https://user-images.githubusercontent.com/31634583/159189268-4fe4ebdc-e8a6-4c03-a140-5dafab79f9e6.png)

![Wordcloud](https://user-images.githubusercontent.com/31634583/159189042-52c00610-c312-447a-a3b3-8b3e16cc55df.png)

![Wordcount](https://user-images.githubusercontent.com/31634583/159189078-d56b8f31-89b8-49ab-ad40-6865ce85b2f5.png)

## Current status
The webscraper is complete and uploading to BQ as well as streamlit tested. Completed profiles are skipped. Profile are scraped in parallel using dask and data is loaded into BQ in batches which size can be set manually (currently 100). The scraper scrapes 100 sites in parallel using dask, and then loads them to BQ. 
Incomplete profiles and already loaded questions in incomplete profiles are skipped.

## Next steps
Add sentiment analysis using Flink.
Orchestrate everything, possibly using Prefect, Make, or a scheduling tool in GCP.
Possibly set of CI using GitHub Actions.

## Current questions
- What is the best way to parallelise my scraper? Now Dask.
- Which tool should I use for orchestration/scheduling?
