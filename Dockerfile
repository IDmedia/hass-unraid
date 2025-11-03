FROM python:3.12-slim

WORKDIR /opt

VOLUME ["/data"]

COPY ./app /opt/app

RUN pip install --no-cache-dir --upgrade -r /opt/app/requirements.txt

CMD ["python", "-m", "app.main"]