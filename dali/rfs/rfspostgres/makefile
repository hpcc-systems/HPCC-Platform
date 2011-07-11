SHELL         =   /bin/bash

TARGET        = bin/rfspostgres

GCCVER        = $(shell gcc -dumpversion)
ISOS64BIT     = $(shell uname -a | egrep -c "x86_64 GNU/Linux")
OSBITS_0      = 32
OSBITS_1      = 64
OSBITS        = $(OSBITS_$(ISOS64BIT))

CCFLAGS       =   -D_PTHREADS -g -D_LARGEFILE64_SOURCE
CC            =   g++
DEFINES       =

INCFLAGS      =  -I. -I.. -I/usr/local/include -I../include -I./include

LIBFLAGS      =  -L/usr/local/lib -L./lib/linux$(OSBITS)_gcc$(GCCVER) -lpq

SRC           = rfspostgres.cpp ../rfs.cpp


OBJDIR        = ./obj
OBJ           = $(OBJDIR)/rfspg.o $(OBJDIR)/rfs.o 


$(TARGET): $(OBJ)
    @mkdir -p bin
    ${CC} ${DEFINES} $(CCFLAGS) $(INCFLAGS) $(LIBFLAGS) $(OBJ) -o $@ 


$(OBJDIR)/%.o: %.cpp
    @mkdir -p obj
    ${CC} ${DEFINES} $(CCFLAGS) $(INCFLAGS) -c $< -o $@
    
$(OBJDIR)/rfs.o: ../rfs.cpp
    @mkdir -p obj
    ${CC} ${DEFINES} $(CCFLAGS) $(INCFLAGS) -c $< -o $@


    

clean:
    -rm -f $(OBJDIR)/*.o $(TARGET)
