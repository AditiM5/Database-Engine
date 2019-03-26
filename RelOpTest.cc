#include "gtest/gtest.h"
#include <iostream>
#include "RelOp.h"
#include "unistd.h"
#include "DBFile.h"
#include "pthread.h"

using namespace std;

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

extern "C" {
	int yyparse(void);   // defined in y.tab.c
	int yyfuncparse(void);   // defined in yyfunc.tab.c
	void init_lexical_parser (char *); // defined in lex.yy.c (from Lexer.l)
	void close_lexical_parser (); // defined in lex.yy.c
	void init_lexical_parser_func (char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
	void close_lexical_parser_func (); // defined in lex.yyfunc.c
}

extern struct AndList *final;
extern struct FuncOperator *finalfunc;
extern FILE *yyin;


int static clear_pipe(Pipe &in_pipe, Schema *schema, bool print) {
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

int static clear_pipe(Pipe &in_pipe, Schema *schema, Function &func, bool print) {
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

// SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c;
// DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c;
// Pipe _ps(pipesz), _p(pipesz), _s(pipesz), _o(pipesz), _li(pipesz), _c(pipesz);
// CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c;
// Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c;
// Function func_ps, func_p, func_s, func_o, func_li, func_c;

// int pAtts = 9;
// int psAtts = 5;
// int liAtts = 16;
// int oAtts = 9;
// int sAtts = 7;
// int cAtts = 8;
// int nAtts = 4;
// int rAtts = 3;

// void init_SF_ps(char *pred_str, int numpgs) {
//     dbf_ps.Open(ps->path());
//     get_cnf(pred_str, ps->schema(), cnf_ps, lit_ps);
//     SF_ps.Use_n_Pages(numpgs);
// }

// void init_SF_p(char *pred_str, int numpgs) {
//     dbf_p.Open(p->path());
//     get_cnf(pred_str, p->schema(), cnf_p, lit_p);
//     SF_p.Use_n_Pages(numpgs);
// }

// void init_SF_s(char *pred_str, int numpgs) {
//     dbf_s.Open(s->path());
//     get_cnf(pred_str, s->schema(), cnf_s, lit_s);
//     SF_s.Use_n_Pages(numpgs);
// }

// void init_SF_o(char *pred_str, int numpgs) {
//     dbf_o.Open(o->path());
//     get_cnf(pred_str, o->schema(), cnf_o, lit_o);
//     SF_o.Use_n_Pages(numpgs);
// }

// void init_SF_li(char *pred_str, int numpgs) {
//     dbf_li.Open(li->path());
//     get_cnf(pred_str, li->schema(), cnf_li, lit_li);
//     SF_li.Use_n_Pages(numpgs);
// }

// void init_SF_c(char *pred_str, int numpgs) {
//     dbf_c.Open(c->path());
//     get_cnf(pred_str, c->schema(), cnf_c, lit_c);
//     SF_c.Use_n_Pages(numpgs);
// }

void static get_cnf (char *input, Schema *left, CNF &cnf_pred, Record &literal) {
	init_lexical_parser (input);
  	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << input << endl;
		exit (1);
	}
	cnf_pred.GrowFromParseTree (final, left, literal); // constructs CNF predicate
	close_lexical_parser ();
}

void static get_cnf (char *input, Schema *left, Schema *right, CNF &cnf_pred, Record &literal) {
	init_lexical_parser (input);
  	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << input << endl;
		exit (1);
	}
	cnf_pred.GrowFromParseTree (final, left, right, literal); // constructs CNF predicate
	close_lexical_parser ();
}

void static get_cnf (char *input, Schema *left, Function &fn_pred) {
		init_lexical_parser_func (input);
  		if (yyfuncparse() != 0) {
			cout << " Error: can't parse your arithmetic expr. " << input << endl;
			exit (1);
		}
		fn_pred.GrowFromParseTree (finalfunc, *left); // constructs CNF predicate
		close_lexical_parser_func ();
}

// test settings file should have the 
// catalog_path, dbfile_dir and tpch_dir information in separate lines
const char *settings = "test.cat";

// donot change this information here
char *catalog_path, *dbfile_dir, *tpch_dir = NULL;

class RelOpTest : public ::testing::Test{

protected:

    // int pipesz = 100;  // buffer sz allowed for each pipe
    // int buffsz = 100;  // pages of memory allowed for operations

    // SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c;
    // DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c;
    // Pipe _ps(pipesz), _p(pipesz), _s(pipesz), _o(pipesz), _li(pipesz), _c(pipesz);
    // CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c;
    // Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c;
    // Function func_ps, func_p, func_s, func_o, func_li, func_c;

    // int pAtts = 9;
    // int psAtts = 5;
    // int liAtts = 16;
    // int oAtts = 9;
    // int sAtts = 7;
    // int cAtts = 8;
    // int nAtts = 4;
    // int rAtts = 3;
};

TEST_F(RelOpTest, test1){

    SelectFile SF_ps;
    DBFile dbf_ps;
    CNF cnf_ps;
    Record lit_ps;
    Pipe _ps(100);

    char *pred_ps = "(ps_supplycost < 100.0)";
    // init_SF_ps(pred_ps, 100);
    dbf_ps.Open("data/partsupp.bin");
    Schema ps_schema("catalog", "partsupp");
    get_cnf(pred_ps, &ps_schema, cnf_ps, lit_ps);
    SF_ps.Use_n_Pages(10);

    dbf_ps.MoveFirst();

    SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);

    int cnt = clear_pipe(_ps, &ps_schema, true);
    cout << "\n\n query1 returned " << cnt << " records \n";
    SF_ps.WaitUntilDone();
    dbf_ps.Close();
    EXPECT_EQ(68, cnt);
}

TEST_F(RelOpTest, test2){
    char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
    SelectFile SF_p;
    DBfile dbf_p;
    CNF cnf_p;
    Record lit_p;
    Pipe ps_p;
    dbf_p.Open("data/part.bin");
    Schema p_schema("catalog", "part");
    get_cnf(pred_p, &p_schema, cnf_p, lit_p);
    SF_p.Use_n_Pages(10);

    Project P_p;
    Pipe _out(100);
    int keepMe[] = {0, 1, 7};
    int numAttsIn = pAtts;
    int numAttsOut = 3;
    P_p.Use_n_Pages(100);

    SF_p.Run(dbf_p, _p, cnf_p, lit_p);
    P_p.Run(_p, _out, keepMe, numAttsIn, numAttsOut);

    SF_p.WaitUntilDone();
    P_p.WaitUntilDone();

    Attribute att3[] = {IA, SA, DA};
    Schema out_sch("out_sch", numAttsOut, att3);

    int count = 0;
    Record *tempRec = new Record;

    int cnt = clear_pipe(_out, &out_sch, true);

    cout << "\n\n query2 returned " << cnt << " records \n";

    dbf_p.Close();

    EXPECT_EQ(0, cnt);
}

