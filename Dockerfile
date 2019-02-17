FROM ubuntu:latest

RUN apt-get -y update && apt-get install -y

RUN apt-get -y install build-essential libpthread-stubs0-dev

RUN apt-get -y install flex bison gdb

COPY catalog Makefile *.l *.y /opt/build/

COPY *.h /opt/build/

COPY *.cc /opt/build/

WORKDIR /opt/build

ARG make_target=main

RUN if [ "${make_target}" = "main" ] ; then make main ; else make test ; fi