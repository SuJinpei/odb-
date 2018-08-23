PROGRAM=odb
INCLUDE=-I. -I./include
LIBPATH=-L./lib
ODIR=obj
CC=g++
LINK_FLAGS=-lodbc -lpthread -ldl -ltcp

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
    Command.cpp \
	Common.cpp \
	Feeder.cpp \
	Loader.cpp \
	Odb.cpp \
    RandSpeed.cpp \
    RunTimeLib.cpp \
	error.cpp \
	main.cpp

OBJ=CommandParser.o \
    Command.o \
	Common.o \
	Feeder.o \
	Loader.o \
	Odb.o \
    RandSpeed.o \
    RunTimeLib.o \
	error.o \
	main.o

$(PROGRAM):$(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LINK_FLAGS) $(INCLUDE) $(LIBPATH)

.cpp.o:
	$(CC) $(CFLAGS) -c -o $@ $< $(INCLUDE)

clean:
	rm -rf *.o
