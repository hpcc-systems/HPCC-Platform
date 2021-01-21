FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get upgrade -y

## Deps
RUN apt-get install -y git wget
RUN apt-get update
RUN apt-get install -y tzdata unzip xvfb libxi6
RUN apt-get install -y default-jdk 

## Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb

## Chrome Driver
RUN wget https://chromedriver.storage.googleapis.com/2.41/chromedriver_linux64.zip
RUN unzip chromedriver_linux64.zip

RUN mv chromedriver /usr/bin/chromedriver
RUN chown root:root /usr/bin/chromedriver
RUN chmod +x /usr/bin/chromedriver

WORKDIR /eclwatch

## Selenium
RUN wget https://selenium-release.storage.googleapis.com/3.141/selenium-server-standalone-3.141.59.jar
RUN wget http://www.java2s.com/Code/JarDownload/testng/testng-6.8.7.jar.zip
RUN unzip testng-6.8.7.jar.zip

## Java Test
WORKDIR /eclwatch
ENV CLASSPATH=".:../selenium-server-standalone-3.141.59.jar:../testng-6.8.7.jar"

COPY ./run.sh .
COPY ./tests ./tests

WORKDIR /eclwatch/tests

RUN javac *.java

WORKDIR /eclwatch

ENTRYPOINT ["./run.sh"]
# CMD ["./run.sh"]

## NodeJS
# RUN apt-get install -y curl
# RUN curl --silent --location https://deb.nodesource.com/setup_14.x | bash -
# RUN apt-get install -y nodejs
# RUN apt-get install -y build-essential

## Node Modules
# COPY ["package.json", "package-lock.json", "./"]
# RUN npm ci

## Sources
# COPY . .

## ECL Watch 
# RUN npm run lint
# RUN npm run build

