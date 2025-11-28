FROM python:3.13-slim

WORKDIR /app
ADD ./requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
RUN pip install pylint

COPY . /app

EXPOSE $PORT

CMD ["python3.13", "app/worker.py"]
