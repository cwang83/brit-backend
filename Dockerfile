FROM python:3.8.13-slim

WORKDIR /app
COPY app.py dynamodb.py requirements.txt ./

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "app.py"]
