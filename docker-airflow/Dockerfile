FROM apache/airflow:2.3.2-python3.8 as airflow

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip3 install -r requirements.txt && \
airflow db upgrade

CMD []

