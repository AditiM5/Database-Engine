
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include "Function.h"
#include "Pipe.h"
#include "Record.h"
#include "RelOp.h"
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

void *producer1(void *args) {
    Pipe *in = (Pipe *)args;
    FILE *tableFile = fopen("data/test.tbl", "r");

    Record temp;
    Schema mySchema("catalog", "lineitem");

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        in->Insert(&temp);
    }

    in->ShutDown();
    pthread_exit(NULL);
}

void *producer2(void *args) {
    Pipe *in = (Pipe *)args;
    FILE *tableFile = fopen("data-1GB/region.tbl", "r");

    Record temp;
    Schema mySchema("catalog", "region");

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        in->Insert(&temp);
    }

    in->ShutDown();
    pthread_exit(NULL);
}

void get_cnf(char *input, Schema *left, CNF &cnf_pred, Record &literal) {
    init_lexical_parser(input);
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF " << input << endl;
        exit(1);
    }
    cnf_pred.GrowFromParseTree(final, left, literal);  // constructs CNF predicate
    close_lexical_parser();
}

void get_cnf(char *input, Schema *left, Schema *right, CNF &cnf_pred, Record &literal) {
    init_lexical_parser(input);
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF " << input << endl;
        exit(1);
    }
    cnf_pred.GrowFromParseTree(final, left, right, literal);  // constructs CNF predicate
    close_lexical_parser();
}

void get_cnf(char *input, Schema *left, Function &fn_pred) {
    init_lexical_parser_func(input);
    if (yyfuncparse() != 0) {
        cout << " Error: can't parse your arithmetic expr. " << input << endl;
        exit(1);
    }
    fn_pred.GrowFromParseTree(finalfunc, *left);  // constructs CNF predicate
    close_lexical_parser_func();
}

int clear_pipe(Pipe &in_pipe, Schema *schema, bool print) {
    Record rec;
    int cnt = 0;
    while (in_pipe.Remove(&rec)) {
        if (print) {
            rec.Print(schema);
        }
        cnt++;
    }
    return cnt;
}

int clear_pipe(Pipe &in_pipe, Schema *schema, Function &func, bool print) {
    Record rec;
    int cnt = 0;
    double sum = 0;
    while (in_pipe.Remove(&rec)) {
        if (print) {
            rec.Print(schema);
        }
        int ival = 0;
        double dval = 0;
        func.Apply(rec, ival, dval);
        sum += (ival + dval);
        cnt++;
    }
    cout << " Sum: " << sum << endl;
    return cnt;
}

int main_1() {
    cout << " query4 \n";

    char *pred_s = "(s_suppkey = s_suppkey)";
    cout << " stage 1A \n";
    // init_SF_s(pred_s, 100);
    DBFile dbfile;
    dbfile.Open("temp/supplier.bin");
    dbfile.MoveFirst();
    init_lexical_parser(pred_s);
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF " << pred_s << endl;
        exit(1);
    }
    CNF cnf_pred;
    Record literal;
    Schema myschema("catalog", "supplier");
    cnf_pred.GrowFromParseTree(final, &myschema, literal);  // constructs CNF predicate
    close_lexical_parser();

    cout << " stage 1B \n";

    // SF_s.Run(dbf_s, _s, cnf_s, lit_s);  // 10k recs qualified
    SelectFile sf;
    Pipe input(1000);
    Pipe output(1000);
    sf.Run(dbfile, output, cnf_pred, literal);
    // sf.WaitUntilDone();

    cout << " stage 1C \n";

    char *pred_ps = "(ps_suppkey = ps_suppkey)";

    // init_SF_ps(pred_ps, 100);
    DBFile dbfile_ps;
    dbfile_ps.Open("temp/partsupp.bin");
    dbfile_ps.MoveFirst();
    init_lexical_parser(pred_ps);
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF " << pred_ps << endl;
        exit(1);
    }
    CNF cnf_pred_ps;
    Record literal_ps;
    Schema myschema_ps("catalog", "partsupp");
    cnf_pred_ps.GrowFromParseTree(final, &myschema_ps, literal_ps);  // constructs CNF predicate
    close_lexical_parser();

    SelectFile sf_ps;
    Pipe input_ps(1000);
    Pipe output_ps(1000);
    sf_ps.Run(dbfile_ps, output_ps, cnf_pred_ps, literal_ps);
    // sf_ps.WaitUntilDone();

    // join
    cout << "Entering join..." << endl;

    cout << " stage 1D \n";
    Join J;
    // left _s
    // right _ps
    Pipe _s_ps(100);
    CNF cnf_p_ps;
    Record lit_p_ps;
    get_cnf("(s_suppkey = ps_suppkey)", &myschema, &myschema_ps, cnf_p_ps, lit_p_ps);

    cout << " stage 2 \n";

    int outAtts = 7 + 5;
    Attribute ps_supplycost = {"ps_supplycost", Double};
    Attribute joinatt[] = {IA, SA, SA, IA, SA, DA, SA, IA, IA, IA, ps_supplycost, SA};
    Schema join_sch("join_sch", outAtts, joinatt);

    cout << " stage 3 \n";

    Sum T;
    // _s (input pipe)
    Pipe _out(1);
    Function func;
    char *str_sum = "(ps_supplycost)";
    get_cnf(str_sum, &join_sch, func);
    func.Print();
    T.Use_n_Pages(1);

    cout << "Stage 4" << endl;

    // sf_ps.WaitUntilDone();

    J.Use_n_Pages(1);

    cout << "stage 5" << endl;

    // sf_ps.WaitUntilDone();
    // usleep(5000000);

    // SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);  // 161 recs qualified
    J.Run(output, output_ps, _s_ps, cnf_p_ps, lit_p_ps);
    T.Run(_s_ps, _out, func);

    // cout << "Join waiting.." << endl;
    // J.WaitUntilDone();

    int count = 0;
    Record temp;
    // while (_out.Remove(&temp)) {
    //     count++;
    // }
    cout << "Count in Test: " << endl;
    T.WaitUntilDone();

    Schema sum_sch("sum_sch", 1, &DA);
    int cnt = clear_pipe(_out, &sum_sch, true);
    cout << " query4 returned " << cnt << " recs \n";
}

int main() {
    char *pred_s = "(s_suppkey = s_suppkey)";
    cout << " stage 1A \n";
    // init_SF_s(pred_s, 100);
    DBFile dbfile;
    dbfile.Open("temp/supplier.bin");
    dbfile.MoveFirst();
    init_lexical_parser(pred_s);
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF " << pred_s << endl;
        exit(1);
    }
    CNF cnf_pred;
    Record literal;
    Schema myschema("catalog", "supplier");
    cnf_pred.GrowFromParseTree(final, &myschema, literal);  // constructs CNF predicate
    close_lexical_parser();

    cout << " stage 1B \n";

    // SF_s.Run(dbf_s, _s, cnf_s, lit_s);  // 10k recs qualified
    SelectFile sf;
    Pipe input(1000);
    Pipe output(1000);
    sf.Run(dbfile, output, cnf_pred, literal);
    // sf.WaitUntilDone();

    int count = 0;
    Record tempRec;
    while (output.Remove(&tempRec)) {
        count++;
        // cout << "\n In this loop for supplier \n";
    }

    cout << "count: " << count << endl;

}
