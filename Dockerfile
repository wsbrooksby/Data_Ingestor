FROM python:3

WORKDIR /Data_Ingestor

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "./Data_Ingestor.py" ]