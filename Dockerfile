FROM python:3.9

WORKDIR /GDLive-Explorer

COPY scraper_requirements.txt .
RUN pip install -r scraper_requirements.txt

COPY ./scraper ./scraper
COPY ./logs ./logs
COPY gcp_key.json .

ENTRYPOINT [ "python3" ]


CMD ["./scraper/main.py" ]

