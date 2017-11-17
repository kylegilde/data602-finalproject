FROM python:3.6
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN git clone https://github.com/kylegilde/data602-assignment2 /usr/src/app/trader-web
EXPOSE 5000
CMD [ "python", "/usr/src/app/trader-web/app.py" ]