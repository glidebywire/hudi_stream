FROM jupyter/scipy-notebook:latest

USER root

ENV HTTP_PROXY="http://10.31.255.65:8080"
ENV HTTPS_PROXY="http://10.31.255.65:8080"
ENV NO_PROXY="10.179.0.20,localhost,*.localhost,k3d_testme"

RUN pip install \
    --proxy=$HTTP_PROXY \
    --trusted-host="10.179.0.20" \
    trino[sqlalchemy]
