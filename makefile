PROGRAM=odb
INCLUDE=.
ODIR=obj
CC=g++
LINK_FLAGS=-lodbc -lpthread

ifeq ($(v), d)
	CFLAGS += -g -D_DEBUG -D_Debug
endif

SRC=CommandParser.cpp \
	Common.cpp \
	Feeder.cpp \
	Loader.cpp \
	Odb.cpp \
	error.cpp \
	main.cpp

OBJ=CommandParser.o \
	Common.o \
	Feeder.o \
	Loader.o \
	Odb.o \
	error.o \
	main.o

$(PROGRAM):$(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LINK_FLAGS)

.cpp.o:
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -rf *.o
