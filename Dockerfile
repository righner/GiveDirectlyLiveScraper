FROM python:3.9.0-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

WORKDIR /GDLive-Explorer

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./etl ./etl
COPY ./streamlit_app ./streamlit_app
#COPY gcp_key.json . #For local-use containers

ENTRYPOINT [ "python" ]

CMD [ "./etl/scraper.py" ]