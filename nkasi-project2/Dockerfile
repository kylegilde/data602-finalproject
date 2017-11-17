FROM python:3

ADD trader-web.py /

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

RUN git clone https://github.com/NNedd/data602-Assignment2

EXPOSE 5000

CMD [ "python", "data602-Assignment2/trader-web.py" ]
