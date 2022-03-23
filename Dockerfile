FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

WORKDIR /GDLive-Explorer

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./etl ./etl
COPY ./streamlit_app ./streamlit_app
#COPY gcp_key.json . #For local-use containers

#Entrypoints will be defined from the outside, since they differ for the scraper and streamlit