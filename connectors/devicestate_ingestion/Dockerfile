FROM python:3.12-alpine

ADD timescaleDB_devicestate_connector.py .

ADD requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "timescaleDB_devicestate_connector.py"]