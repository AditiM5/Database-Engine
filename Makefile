CC = g++ -O2 -Wno-deprecated -Wno-unused-result -Wno-write-strings -Wno-format-overflow
MAKE = make
UNAME_S := $(shell uname -s)

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o test.o
ifeq ($(UNAME_S),Darwin)
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o test.o -ll -lpthread
else
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o test.o -lfl -lpthread
endif
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o main.o
ifeq ($(UNAME_S),Darwin)
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o main.o -ll -lpthread
else
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o RecordPageNum.o PriorityQueue.o y.tab.o lex.yy.o main.o -lfl -lpthread
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

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h

rebuild_and_run:
	$(MAKE) main
	./main

clean_rebuild_run:
	$(MAKE) clean
	$(MAKE) rebuild_and_run
