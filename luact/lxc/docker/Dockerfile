FROM umegaya/luact:deps
MAINTAINER Takehiro Iyatomi <iyatomi@gmail.com>
ADD ./luact/lxc /tmp/lxc
RUN /tmp/lxc/build/core.sh
RUN rm -rf /tmp/lxc
RUN apt-get -y autoremove && apt-get -y autoclean && apt-get -y clean
