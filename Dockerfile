FROM python:alpine3.9
COPY . /Twitter-Streaming
WORKDIR /Twitter-Streaming
RUN pip install --upgrade pip
RUN apk add --no-cache --update \
    python3 python3-dev gcc \
    gfortran musl-dev g++ \
    libffi-dev openssl-dev \
    libxml2 libxml2-dev \
    libxslt libxslt-dev \
    libjpeg-turbo-dev zlib-dev
RUN pip install -r requirements.txt
# RUN python ./src/ingest.py
EXPOSE 8050
# ENTRYPOINT [ "python" ]
CMD ["python", "app.py"]