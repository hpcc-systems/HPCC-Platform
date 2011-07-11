#!/bin/bash

OUTPUT=""
ARCH=`uname -m`
ARCH2=${ARCH}
case "$ARCH" in
  "x86_64")
     ARCH="x86_64"
     ARCH2="amd64"
     ;;
   "i386" | "i686")
     ARCH="i386"
     ARCH2="i386"
     ;;
esac


if [ -e /etc/debian_version ]; then
  if [ -e /etc/lsb-release ]; then
    . /etc/lsb-release
    OUTPUT="${DISTRIB_CODENAME}_${ARCH2}"
  else
    case `cat /etc/debian_version` in
      5.*)
        OUTPUT="lenny_${ARCH2}"
        ;;
      "sid")
        OUTPUT="sid_${ARCH2}"
        ;;
    esac
  fi
elif [ -e /etc/redhat-release ]; then
  if [ -x /bin/rpm ]; then
    OUTPUT="${OUTPUT}\npackage=rpm"
    OS_GROUP=`/bin/rpm -q --qf "%{NAME}" --whatprovides /etc/redhat-release | sed 's/-release.*//' |  tr '[A-Z]' '[a-z]'`
    REDHAT_VERSION=`/bin/rpm -q --qf "%{VERSION}" --whatprovides /etc/redhat-release`
    case "$OS_GROUP" in
      "centos" | "fedora")
        OUTPUT="el${REDHAT_VERSION}.${ARCH}"
        ;;
      "redhat")
        REDHAT_RELEASE=`/bin/rpm -q --qf "%{RELEASE}" --whatprovides /etc/redhat-release| cut -d. -f1`
        OUTPUT="el${REDHAT_VERSION}.${ARCH}"
        ;;
      esac
    fi
elif [ -e /etc/SuSE-release ]; then
  if [ -x /bin/rpm ]; then
    OS_GROUP=`/bin/rpm -q --qf "%{NAME}" --whatprovides /etc/SuSE-release | sed 's/-release.*//' |  tr '[A-Z]' '[a-z]'`
    REDHAT_VERSION=`/bin/rpm -q --qf "%{VERSION}" --whatprovides /etc/SuSE-release`
      case "$OS_GROUP" in
        "opensuse" )
          OUTPUT="suse${REDHAT_VERSION}.${ARCH}"
          ;;
      esac
  fi
elif [ -e /etc/gentoo-release ]; then
  OUTPUT="gentoo"
else
  exit 0
fi

echo -n $OUTPUT

