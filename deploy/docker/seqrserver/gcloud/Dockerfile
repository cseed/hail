FROM openjdk:8-jdk

RUN (cd /usr/local && \
  wget -nv http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz && \
  tar xzf /usr/local/spark-2.0.2-bin-hadoop2.7.tgz)

RUN apt-get update
RUN apt-get install -y python-pip
RUN apt-get install -y python-dev
RUN pip install decorator
RUN pip install numpy

COPY hail /hail

RUN useradd -m -s /bin/bash seqrserver

COPY seqrserver.py /
COPY entrypoint.sh /

WORKDIR /

CMD [ "/entrypoint.sh" ]
