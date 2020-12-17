FROM python:3.8
COPY requirements.txt requirements-dev.txt ./
RUN pip3 install --no-cache-dir -r requirements-dev.txt
ENTRYPOINT ["python3"]
