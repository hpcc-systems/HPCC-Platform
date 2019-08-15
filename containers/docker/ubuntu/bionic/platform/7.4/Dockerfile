From hpccsystems/hpcc-dev:7-bionic AS bionic-builder
# Usage: sudo docker build -t <image tag> --build-arg branch=<HPCC branch or tag> --build-arg owner=<project owner> .
# The default branch is "master". The default github project owner is "hpcc-systems".
# user/password is for clone non-public project such as LN.
ARG branch=
ARG owner="hpcc-systems"
ARG user=
ARG password=

WORKDir /hpcc-dev
RUN set -ex; \
   if [ -n "${user}" ] ; \
   then \
      echo "machine github.com login ${user} password ${password}" > ~/.netrc ; \
   else : ;  fi; \
   git clone https://github.com/${owner}/HPCC-Platform.git; \
   cd HPCC-Platform; \
   if [ -n "${branch}" ] ; \
   then \
      git tag | grep ${branch}; \
      if [ $? -eq 0 ]; \
      then \
         git fetch && git fetch --tags; \
      fi; \
      git checkout ${branch}; \
   else : ; fi; \
   git submodule update --init --recursive; \
   cd ..; \
   mkdir -p build; \
   cd build; \
   rm -rf * ; \
   cmake ../HPCC-Platform; \
   make -j 4 ; \
   make package


From hpccsystems/hpcc-base:7-bionic

WORKDir /Downloads
COPY --from=0 /hpcc-dev/build/hpccsystems*.deb .
RUN set -ex; \
   apt-get update -y; \
   apt-get install -y iputils-ping curl xterm libarchive13 ; \
   groupadd hpcc && useradd -s /bin/bash -r -m -d /home/hpcc -g hpcc -c "hpcc Runtime User" hpcc ; \
   dpkg -i hpccsystems*.deb; \
   rm -rf  hpccsystems*.deb

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/usr/sbin/sshd", "-D"]
