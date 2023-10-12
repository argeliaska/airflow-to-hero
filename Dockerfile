FROM    apache/airflow:2.7.2rc1-python3.8
COPY    requirements.txt /requirements.txt
RUN     pip install --user --upgrade pip
RUN     pip install --no-cache-dir --user -r /requirements.txt

# docker build . --tag extending_airflow:latest