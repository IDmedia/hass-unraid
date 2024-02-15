FROM python:3.9-slim

VOLUME ["/data"]
COPY ./app /app
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
CMD ["python", "/app/main.py"]