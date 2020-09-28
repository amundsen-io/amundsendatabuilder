FROM python:3.7-slim as base
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip3 install -r requirements.txt

COPY . /app
RUN python3 setup.py install

ENTRYPOINT [ "python3",  "example/scripts/minio_core_loader.py" ]
