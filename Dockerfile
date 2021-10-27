FROM python:3.9-slim
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src/app.py .
COPY src/amqp_setup.py .
CMD ["python", "./app.py"]
