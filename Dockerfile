FROM ubuntu:focal

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -y g++ make cmake curl ca-certificates lsb-release wget gnupg git && \
    wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    dpkg -i apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get install -y libarrow-dev && \
    apt-get update && apt-get install -y python3 python3-pip && \
    python3 -m pip install -U pip && \
    python3 -m pip install pyarrow==3.0.0

ADD . /src

WORKDIR /src
RUN cmake -DCMAKE_BUILD_TYPE=Release . && \
    make -j && \
    make install

WORKDIR /
