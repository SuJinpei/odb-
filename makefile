PROGRAM=odb
INCLUDE=-I.
LIBPATH=
ODIR=obj
CC=g++
LINK_FLAGS=-lodbc -lpthread

ifeq ($(cut), 1)
INCLUDE += -I./include
LIBPATH += -L./lib
LINK_FLAGS += -lstcp
CFLAGS += -D_TEST
endif

ifeq ($(v), d)
	CFLAGS += -g -O0 -D_DEBUG -D_Debug
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
	$(CC) $(CFLAGS) -o $@ $^ $(LINK_FLAGS) $(INCLUDE) $(LIBPATH)

.cpp.o:
	$(CC) $(CFLAGS) -c -o $@ $< $(INCLUDE)

clean:
	rm -rf *.o
