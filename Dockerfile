FROM python:3.7

WORKDIR /code

# copy the dependencies file to the working directory
COPY code/requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the configuration
COPY code/config.yaml .

# command to run on container start
CMD [ "python", "./app.py" ]
