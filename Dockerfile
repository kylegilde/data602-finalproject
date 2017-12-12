FROM jupyter/base-notebook
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt
EXPOSE 8888
CMD ["jupyter", "notebook", "--no-browser", "--allow-root"]
