FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

WORKDIR /GDLive-Explorer

COPY requirements.txt .
RUN pip install -r requirements.txt
## Until this day streamlit uses a GCP reserved URL for their health-checks, which needs to beworked-around with this hack : https://discuss.streamlit.io/t/has-anyone-deployed-to-google-cloud-platform/931/23  
RUN find /usr/local/lib/python3.9/site-packages/streamlit -type f \( -iname \*.py -o -iname \*.js \) -print0 | xargs -0 sed -i 's/healthz/health-check/g'

COPY ./etl ./etl
COPY ./streamlit_app ./streamlit_app
#COPY gcp_key.json . #For local-use containers

#Entrypoints will be defined from the outside, since they differ for the scraper and streamlit