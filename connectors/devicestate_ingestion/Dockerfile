FROM python:3.12-slim

ADD devicestate_connector.py .

ADD requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "devicestate_connector.py"]