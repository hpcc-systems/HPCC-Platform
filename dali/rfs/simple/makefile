SHELL       =   /bin/bash

CCFLAGS     =   -D_PTHREADS -g -D_LARGEFILE64_SOURCE
CC          =   g++
DEFINES     =

INCFLAGS    =  -I. -I.. -I/usr/local/include 

LIBFLAGS    =  -I/usr/local/lib

SRC         = simplerfs.cpp ../rfs.cpp

OBJ     =   $(SRC:.cpp=.o)

%: %.cpp
    ${CC} ${DEFINES} $(CCFLAGS) $(INCFLAGS) $(LIBFLAGS) $< -o $@ 

%.o: %.cpp
    ${CC} ${DEFINES} $(CCFLAGS) $(INCFLAGS) -c $< -o $@

simplerfs: $(OBJ)
    ${CC} ${DEFINES} $(CCFLAGS) $(INCFLAGS) $(LIBFLAGS) \
        $(OBJ) -o $@ 

clean:
    -rm -f *.o core* $(OBJ)
