FROM python:3.13-slim

RUN apt-get update
RUN apt-get install -y curl

WORKDIR /app
ADD ./requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
RUN pip install pylint

COPY . /app

EXPOSE $PORT

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app.server:app"]
