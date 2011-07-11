################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################


host-type := $(shell arch)
ifeq ($(host-type),sun4)
OS=SOLARIS
else
OS=LINUX
endif

SOLARIS_CC=CC
SOLARIS_AR=CC -xar -o
SOLARIS_INCLUDEPATH=-I/usr/local/ssl/include
SOLARIS_LIBS=-lz -lpthread -lsocket
SOLARIS_STATICLIBS=/usr/local/ssl/lib/libssl.a /usr/local/ssl/lib/libcrypto.a

LINUX_CC=g++
LINUX_AR=ar cru
LINUX_LIBS=-lz -lpthread -ldl
LINUX_INCLUDEPATH=-I/usr/src/redhat/BUILD/openssl-0.9.7a/include -I/usr/kerberos/include
LINUX_STATICLIBS=/usr/src/redhat/BUILD/openssl-0.9.7a/libcrypto.a
CC=$($(OS)_CC)

LIBSRC=zcrypt.cpp aes.cpp
LIBOBJ=$(destdir)/obj/zcrypt.o $(destdir)/obj/aes.o $(destdir)/obj/mutex.o $(destdir)/obj/base64.o

LIBS=-lz -lpthread -lsocket

all : 
    @gmake setup "destdir=./release"
ifeq ($(host-type),sun4)
    @gmake zcrypt "OPT=-fast -xtarget=generic" "destdir=./release"
    @gmake zcrypttest "OPT=-fast -xtarget=generic" "destdir=./release"
else
    @gmake zcrypt "OPT=-O" "destdir=./release"
    @gmake zcrypttest "OPT=-O" "destdir=./release"
endif

DEBUG:
    @gmake setup "destdir=./debug"
    @gmake zcrypt "OPT=-g" "destdir=./debug"
    @gmake zcrypttest "OPT=-g" "destdir=./debug"
    
zcrypt:
    @gmake $(destdir)/lib/libzcrypt.a

$(destdir)/lib/libzcrypt.a: $(LIBOBJ)
    $($(OS)_AR) $(destdir)/lib/libzcrypt.a $(LIBOBJ) 
    ranlib $(destdir)/lib/libzcrypt.a

zcrypttest:
    @gmake $(destdir)/bin/zcrypttest

$(destdir)/bin/zcrypttest: $(destdir)/obj/main.o $(destdir)/lib/libzcrypt.a
    $(CC) $($(OS)_LIBS) -o $@ $(destdir)/obj/main.o $(destdir)/lib/libzcrypt.a $($(OS)_STATICLIBS)

$(destdir)/obj/zcrypt.o : zcrypt.cpp zcrypt.ipp zcrypt.hpp aes.hpp mutex.ipp base64.ipp
    $(CC) $(OPT) -c $($(OS)_INCLUDEPATH) -o $@ $<
    
$(destdir)/obj/aes.o: aes.cpp aes.hpp
    $(CC) $(OPT) -c $($(OS)_INCLUDEPATH) -o $@ $<

$(destdir)/obj/base64.o: base64.cpp base64.ipp
    $(CC) $(OPT) -c $($(OS)_INCLUDEPATH) -o $@ $<

$(destdir)/obj/mutex.o: mutex.cpp mutex.ipp
    $(CC) $(OPT) -c $($(OS)_INCLUDEPATH) -o $@ $<

$(destdir)/obj/main.o: main.cpp zcrypt.hpp
    $(CC) $(OPT) -c $($(OS)_INCLUDEPATH) -o $@ $<
setup:
    mkdir -p $(destdir)/lib; mkdir -p $(destdir)/bin; mkdir -p $(destdir)/obj;
    mkdir -p $(destdir)/include; cp zcrypt.hpp $(destdir)/include
clean:
    rm -rf ./release; rm -rf ./debug

