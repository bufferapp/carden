FROM python:3

RUN pip install pymongo dnspython google-cloud-bigquery stacklogging

ADD main.py /app/

WORKDIR /app

CMD ["python", "main.py"]
