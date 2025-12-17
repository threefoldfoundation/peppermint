FROM python:3.12-alpine

RUN mkdir /peppermint
COPY . /peppermint
RUN pip install -r /peppermint/requirements.txt

WORKDIR /peppermint
