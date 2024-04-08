#!/bin/bash

print_usage(){
    echo "usage: getpackagerevisionarch.sh [-n|--noarch] "
    exit 1
}

NOARCH=0
TEMP=`/usr/bin/getopt -o nh --long help,noarch -n 'getpackagerevisionarch.sh' -- "$@"`
if [ $? != 0 ] ; then echo "Failure to parse commandline." >&2 ; exit 1 ; fi
eval set -- "$TEMP"
while true ; do
    case "$1" in
	-n|--noarch) NOARCH=1
	    shift ;;
        -h|--help) print_usage
                   shift ;;
        --) shift ; break ;;
        *) print_usage ;;
    esac
done

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
   arm*)
     ARCH="arm"
     ARCH2="arm"
     ;;
esac


if [ -e /etc/debian_version ]; then
  if [ -e /etc/lsb-release ]; then
    . /etc/lsb-release
    if [ ${NOARCH} -eq 0 ]; then
        OUTPUT="${DISTRIB_CODENAME}_${ARCH2}"
    else
        OUTPUT="${DISTRIB_CODENAME}"
    fi
  else
    case `cat /etc/debian_version` in
      5.*)
        if [ ${NOARCH} -eq 0 ]; then
            OUTPUT="lenny_${ARCH2}"
        else
            OUTPUT="lenny"
        fi
        ;;
      6.*)
        if [ ${NOARCH} -eq 0 ]; then
            OUTPUT="squeeze_${ARCH2}"
        else
            OUTPUT="squeeze"
        fi
        ;;
      8.*)
        if [ ${NOARCH} -eq 0 ]; then
            OUTPUT="jessie_${ARCH2}"
        else
            OUTPUT="jessie"
        fi
        ;;
      "sid")
        if [ ${NOARCH} -eq 0 ]; then
            OUTPUT="sid_${ARCH2}"
        else
            OUTPUT="sid"
        fi
        ;;
    esac
  fi
elif [ -e /etc/redhat-release ]; then
  if [ -x /bin/rpm ]; then
    OUTPUT="${OUTPUT}\npackage=rpm"
    OS_GROUP=`/bin/rpm -q --qf "%{NAME}" --whatprovides /etc/redhat-release | sed 's/-release.*//' |  tr '[A-Z]' '[a-z]'`
    REDHAT_VERSION=`/bin/rpm -q --qf "%{VERSION}" --whatprovides /etc/redhat-release | cut -f1 -d"."`
    case "$OS_GROUP" in
      "centos"* | "fedora" | "rocky"* )
        if [ ${NOARCH} -eq 0 ]; then
            OUTPUT="el${REDHAT_VERSION}.${ARCH}"
        else
            OUTPUT="el${REDHAT_VERSION}"
        fi
        ;;
      "redhat")
        REDHAT_RELEASE=`/bin/rpm -q --qf "%{RELEASE}" --whatprovides /etc/redhat-release| cut -d. -f1`
        if [ ${NOARCH} -eq 0 ]; then
            OUTPUT="el${REDHAT_VERSION}.${ARCH}"
        else
            OUTPUT="el${REDHAT_VERSION}"
        fi
        ;;
      esac
    fi
elif [ -e /etc/SuSE-release ]; then
  if [ -x /bin/rpm ]; then
    OS_GROUP=`/bin/rpm -q --qf "%{NAME}" --whatprovides /etc/SuSE-release | sed 's/-release.*//' |  tr '[A-Z]' '[a-z]'`
    REDHAT_VERSION=`/bin/rpm -q --qf "%{VERSION}" --whatprovides /etc/SuSE-release`
      case "$OS_GROUP" in
        "opensuse" )
          if [ ${NOARCH} -eq 0 ]; then
              OUTPUT="suse${REDHAT_VERSION}.${ARCH}"
          else
              OUTPUT="suse${REDHAT_VERSION}"
          fi
          ;;
      esac
  fi
elif [ -e /etc/system-release ]; then
  if [ -x /bin/rpm ]; then
      OS_GROUP=$(grep -i "Linux" /etc/system-release | awk '{ print  $1}')
      AMZN_VERSION=$(grep -i "Linux" /etc/system-release | awk '{ print  $4}')
      case "$OS_GROUP" in
        "Amazon" )
          if [ ${NOARCH} -eq 0 ]; then
              OUTPUT="amzn${AMZN_VERSION}.${ARCH}"
          else
              OUTPUT="amzn${AMZN_VERSION}"
          fi
          ;;

      esac
  fi
elif [ -e /etc/gentoo-release ]; then
  OUTPUT="gentoo"
else
  exit 0
fi

echo -n $OUTPUT

