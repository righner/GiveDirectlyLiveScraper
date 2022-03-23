FROM python:3.9-slim

WORKDIR /GDLive-Explorer

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./etl ./etl
COPY ./streamlit ./streamlit
#COPY gcp_key.json . #For local-use containers

#Entrypoints will be defined from the outside, since they differ for the scraper and streamlit