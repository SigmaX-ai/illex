ARG UBUNTU_TAG=focal
FROM ubuntu:$UBUNTU_TAG as arrow

ENV DEBIAN_FRONTEND noninteractive

ARG ARROW_VERSION=3.0.0
RUN apt-get update && \
    apt-get install -y wget lsb-release gnupg && \
    wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    dpkg -i apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get install -y libarrow-dev=$ARROW_VERSION-1 && \
    apt-get remove -y --purge wget lsb-release gnupg apache-arrow-archive-keyring && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

FROM arrow as build
RUN apt-get update && \
    apt-get install -y g++ make cmake git
ADD . /src
WORKDIR /release
RUN cmake -DCMAKE_BUILD_TYPE=Release /src && \
    make -j

FROM arrow
EXPOSE 10197
COPY --from=build /release/illex /illex
ENTRYPOINT [ "/illex" ]
CMD ["--help"]
