FROM jupyter/scipy-notebook:latest

USER root
COPY requirements-base.txt /tmp/requirements-base.txt
RUN pip install --no-cache-dir -r /tmp/requirements-base.txt

USER jovyan
WORKDIR /home/jovyan