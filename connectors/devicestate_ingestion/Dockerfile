FROM python:3.12-slim

ADD postgresDB_devicestate_connector.py .

ADD requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "postgresDB_devicestate_connector.py"]