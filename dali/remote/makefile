
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
       CC $(CCFLAGS) $(INCFLAGS) -c $(*F).cpp -o $@;   \
    fi

clean:
    @rm -rf $(OBJDIR) *.log *.trc 


