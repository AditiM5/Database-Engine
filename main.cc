
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include "Function.h"
#include "Pipe.h"
#include "Record.h"
#include "RelOp.h"
#include "Statistics.h"
#include "pthread.h"

using namespace std;

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

// extern "C" {
// int yyparse(void);  // defined in y.tab.c
// }

extern "C" {
int yyparse(void);                      // defined in y.tab.c
int yyfuncparse(void);                  // defined in yyfunc.tab.c
void init_lexical_parser(char *);       // defined in lex.yy.c (from Lexer.l)
void close_lexical_parser();            // defined in lex.yy.c
void init_lexical_parser_func(char *);  // defined in lex.yyfunc.c (from Lexerfunc.l)
void close_lexical_parser_func();       // defined in lex.yyfunc.c
}

extern struct AndList *final;
extern struct FuncOperator *finalfunc;
extern FILE *yyin;

// extern struct AndList *final;

int main() {
    Statistics s;
    char *relName[] = {"supplier", "customer", "nation"};

    // s.Read(fileName);

    s.AddRel(relName[0], 10000);
    s.AddAtt(relName[0], "s_nationey", 25);

    s.AddRel(relName[1], 150000);
    s.AddAtt(relName[1], "c_custkey", 150000);
    s.AddAtt(relName[1], "c_nationkey", 25);

    s.AddRel(relName[2], 25);
    s.AddAtt(relName[2], "n_nationkey", 25);

    s.CopyRel("nation", "n1");
    s.CopyRel("nation", "n2");
    s.CopyRel("supplier", "s");
    s.CopyRel("customer", "c");

    s.Write("stat_file.txt");

    Statistics stat;

    stat.Read("stat_file.txt");
    stat.Write("stat_file_new.txt");
}
