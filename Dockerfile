FROM jupyter/base-notebook
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN git clone https://github.com/kylegilde/data602-finalproject /usr/src/app/final
EXPOSE 8888
CMD ["jupyter", "notebook", "--no-browser", "--allow-root"]
