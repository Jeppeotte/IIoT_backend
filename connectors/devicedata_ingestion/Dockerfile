FROM python:3.12-slim

ADD devicedata_connector.py .

ADD requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "devicedata_connector.py"]