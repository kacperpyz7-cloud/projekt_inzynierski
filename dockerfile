FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary

COPY iot_generator.py .

CMD ["python", "-u", "iot_generator.py"]