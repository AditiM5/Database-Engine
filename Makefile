CC = g++ -O2 -Wno-deprecated -Wno-unused-variable -Wno-unused-parameter -Wno-unused-result -Wno-write-strings -Wno-format-overflow -isystem $(GTEST_DIR)/include -Wall -Wextra -pthread
MAKE = make
UNAME_S := $(shell uname -s)

# Google test setup

GTEST_DIR = /usr/src/gtest
USER_DIR = .

GTEST_HEADERS = /usr/include/gtest/*.h \
                /usr/include/gtest/internal/*.h

# Flags passed to the preprocessor.
CPPFLAGS += -isystem $(GTEST_DIR)/include

# Flags passed to the C++ compiler.
CXXFLAGS += -g -Wall -Wextra -pthread

GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

# Google test setup ends

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Pipe.o BigQ.o GenericDBFile.o HeapFile.o SortedFile.o DBFile.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o test.o
ifeq ($(UNAME_S),Darwin)
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Pipe.o BigQ.o GenericDBFile.o HeapFile.o SortedFile.o DBFile.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o test.o -ll -lpthread
else
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Pipe.o BigQ.o GenericDBFile.o HeapFile.o SortedFile.o DBFile.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o test.o -lfl -lpthread
endif
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o HeapFile.o SortedFile.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o main.o
ifeq ($(UNAME_S),Darwin)
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o HeapFile.o SortedFile.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o main.o -ll -lpthread
else
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o HeapFile.o SortedFile.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o main.o -lfl -lpthread
endif
	
test.o: test.cc
	$(CC) -g -c test.cc

main.o: main.cc
	$(CC) -g -c main.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

PriorityQueue.o: PriorityQueue.cc
	$(CC) -g -c PriorityQueue.cc

RecordPageNum.o: RecordPageNum.cc
	$(CC) -g -c RecordPageNum.cc

HeapFile.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc

SortedFile.o: SortedFile.cc
	$(CC) -g -c SortedFile.cc

GenericDBFile.o: GenericDBFile.cc
	$(CC) -g -c GenericDBFile.cc

y.tab.o: Parser.y
	yacc -d Parser.y
ifeq ($(UNAME_S), Darwin)
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
else
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
endif
	g++ -c -Wno-write-strings y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

# Google test targets

# All tests produced by this Makefile.  Remember to add new tests you
# created to the list.
TESTS = BigQTest

gtest : $(TESTS)

gtest-all.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc

gtest_main.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest_main.cc

gtest.a : gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest_main.a : gtest-all.o gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

# Builds a sample test.  A test should link with either gtest.a or
# gtest_main.a, depending on whether it defines its own main()
# function.

BigQTest.o : $(USER_DIR)/BigQTest.cc $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/BigQTest.cc

BigQTest : Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o BigQTest.o gtest_main.a
	$(CC) $^ -lfl -lpthread -o $@

# end Google test targets

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h

clean_gtest:
	rm -f $(TESTS) gtest.a gtest_main.a *.o

rebuild_and_run:
	$(MAKE) main
	./main

clean_rebuild_run:
	$(MAKE) clean
	$(MAKE) rebuild_and_run
