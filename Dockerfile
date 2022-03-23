FROM python:3.9-slim

WORKDIR /GDLive-Explorer

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./etl ./etl
COPY ./streamlit ./streamlit
COPY ./logs ./logs
COPY gcp_key.json .

ENTRYPOINT [ "python3" ]

#CMD ["./etl/main.py" ]

