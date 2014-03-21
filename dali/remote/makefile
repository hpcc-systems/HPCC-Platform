
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

SHELL      = /usr/bin/ksh

PROGRAM    = daliservix
API        = 
SYBASE     = /opt/sybase
HPOV       = /opt/OV

APILIBS    =
SYBLIBS    = -lcs -lct -ltcl -lcomn -lintl
HOVLIBS    = -lopc -lnsp -lnspn
SYSLIBS    = -lsocket -lxnet -lpthread -lposix4 -lnsl
PARLIBS    = -ly -ll

OBJDIR     = obj

INCFLAGS  += -I. -I$(SYBASE)/include -I$(HPOV)/include -I/opt/SUNWspro/WS6/include/f90

LDLIBS    += $(APILIBS) $(SYSLIBS) $(PARLIBS)

SOURCES    = daliservix.cpp

HEADERS    = *.h *.hpp

OBJECTS    = $(SOURCES:%.cpp=$(OBJDIR)/%.o)

CCFLAGS    = -w $(DB) -mt -D_PTHREADS -Dsun


all: daliservix
	@echo Build Complete


daliservix: $(OBJECTS)
	CC $(CCFLAGS) $(LDFLAGS) -o $@ $(OBJECTS) $(LDLIBS);

%.o: $(SOURCES)
	@if test "$(*F).cpp" -nt "$@"; then   \
	echo "     Compiling $(*F).cpp";   \
	mkdir -p $(OBJDIR); \
	CC $(CCFLAGS) $(INCFLAGS) -c $(*F).cpp -o $@;   \
	fi

clean:
	@rm -rf $(OBJDIR) *.log *.trc


