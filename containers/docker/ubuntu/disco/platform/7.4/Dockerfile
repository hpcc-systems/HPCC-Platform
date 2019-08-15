From hpccsystems/hpcc-dev:7-disco AS disco-builder
# Usage: sudo docker build -t <image tag> --build-arg branch=<HPCC branch or tag> --build-arg owner=<project owner> .
# The default branch is "master". The default github user is "hpcc-systems".
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
      tag=$(git tag | grep ${branch}) || :; \
      if [ -n "${tag}" ]; \
      then \
         git fetch && git fetch --tags; \
      else : ; \
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


From hpccsystems/hpcc-base:7-disco

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
