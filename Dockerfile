FROM python:3

RUN pip install pymongo dnspython google-cloud-bigquery

ADD main.py bq_io.py /app/

WORKDIR /app

CMD ["python", "main.py"]
