FROM ubuntu:latest

RUN apt-get clean && rm -rfv /var/lib/apt/lists/* && apt update

RUN apt-get -y update && apt-get install -y

RUN apt-get -y install build-essential libpthread-stubs0-dev cmake

RUN apt-get -y --fix-missing install flex bison

RUN apt-get -y install libgtest-dev

RUN apt-get -y install gdb

WORKDIR /usr/src/gtest

RUN cmake CMakeLists.txt && make && cp *.a /usr/lib

COPY catalog Makefile *.l *.y /opt/build/

COPY *.h /opt/build/

COPY *.cc /opt/build/

WORKDIR /opt/build

ARG make_target=main

RUN if [ "${make_target}" = "main" ] ; then make main ; else make test.out ; fi

# RUN if [ "${make_target}" = "test.out" ] ; the make test.out ; fi

# RUN if [ "${make_target}" = "gtest" ] ; the make gtest ; fi