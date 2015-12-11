FROM jupyter/all-spark-notebook
MAINTAINER Jongyeol Yang <yangjy0113@gmail.com>

# Install spark 1.5.2
RUN mkdir /app
WORKDIR /app
RUN wget http://d3kbcqa49mib13.cloudfront.net/spark-1.5.2-bin-hadoop2.6.tgz
RUN tar -zxf spark-1.5.2-bin-hadoop2.6.tgz

# set environment path
ENV SPARKHOME /app/spark-1.5.2-bin-hadoop2.6
ENV SPARKSCALAEX ls /app/spark-1.5.2-bin-hadoop2.6/examples/src/main/scala/org/apache/spark/examples

RUN cp -r /home/$NB_USER/.jupyter /root/

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
RUN apt-get update
RUN apt-get install -y sbt

