FROM apache/airflow:2.6.3
COPY requirements.txt .


ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1  
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt