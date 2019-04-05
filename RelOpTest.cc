#include "gtest/gtest.h"
#include <iostream>
#include "RelOp.h"
#include "unistd.h"
#include "DBFile.h"
#include "Pipe.h"
#include "pthread.h"
#include <string>

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
};

TEST_F(RelOpTest, test1){

    SelectFile SF_ps;
    DBFile dbf_ps;
    CNF cnf_ps;
    Record lit_ps;
    Pipe _ps(100);

    char *pred_ps = "(ps_supplycost < 100.0)";
    dbf_ps.Open("data/partsupp.bin");
    Schema ps_schema("catalog", "partsupp");
    get_cnf(pred_ps, &ps_schema, cnf_ps, lit_ps);
    SF_ps.Use_n_Pages(10);

    dbf_ps.MoveFirst();

    SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);

    int cnt = clear_pipe(_ps, &ps_schema, false);
    cout << "Query1 returned " << cnt << " records \n";
    SF_ps.WaitUntilDone();
    dbf_ps.Close();
    EXPECT_EQ(68, cnt);
}

TEST_F(RelOpTest, test2){
    char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
    SelectFile SF_p;
    DBFile dbf_p;
    CNF cnf_p;
    Record lit_p;
    Pipe ps_p(100), _p(100);
    dbf_p.Open("data/part.bin");
    Schema p_schema("catalog", "part");
    get_cnf(pred_p, &p_schema, cnf_p, lit_p);
    SF_p.Use_n_Pages(10);

    Project P_p;
    Pipe _out(100);
    int keepMe[] = {0, 1, 7};
    int numAttsIn = 9;
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

    cout << "Query2 returned " << cnt << " records \n";

    dbf_p.Close();

    EXPECT_EQ(1, cnt);
}

TEST_F(RelOpTest, test3){ 
    char *pred_s = "(s_suppkey = s_suppkey)";

    CNF cnf_s;
    Record lit_s;
    Schema s_schema("catalog", "supplier");
    get_cnf(pred_s, &s_schema, cnf_s, lit_s);

    DBFile dbf_s;
    dbf_s.Open("data/supplier.bin");

    Pipe _s(100), _out(100);


    SelectFile SF_s;
    Sum T;
    Function func;
    char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
    get_cnf(str_sum, &s_schema, func);
    T.Use_n_Pages(1);
    SF_s.Run(dbf_s, _s, cnf_s, lit_s);
    T.Run(_s, _out, func);

    Schema out_sch("out_sch", 1, &DA);

    Record tempRec;

    _out.Remove(&tempRec);

    string s = tempRec.ToString(&out_sch);
    s.pop_back();

    SF_s.WaitUntilDone();
    T.WaitUntilDone();

    dbf_s.Close();

    EXPECT_EQ(s, "88860.3865");
}

TEST_F(RelOpTest, test4){ 

    DBFile dbf_s, dbf_ps;
    CNF cnf_s, cnf_ps, cnf_p_ps;
    Record lit_s, lit_ps, lit_p_ps;
    Schema s_schema("catalog", "supplier"), ps_schema("catalog", "partsupp");
    Pipe _s(100), _ps(100), _s_ps(100), _out(100);
    SelectFile SF_s, SF_ps;
    Join J;
    Sum T;
    Function func;

    char *pred_s = "(s_suppkey = s_suppkey)";
    get_cnf(pred_s, &s_schema, cnf_s, lit_s);
    dbf_s.Open("data/supplier.bin");
    SF_s.Run(dbf_s, _s, cnf_s, lit_s);

    char *pred_ps = "(ps_suppkey = ps_suppkey)";
    get_cnf(pred_ps, &ps_schema, cnf_ps, lit_ps);
    dbf_ps.Open("data/partsupp.bin");
    SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);

    get_cnf("(s_suppkey = ps_suppkey)", &s_schema, &ps_schema, cnf_p_ps, lit_p_ps);

    int outAtts = 7 + 5;
    Attribute ps_supplycost = {"ps_supplycost", Double};
    Attribute joinatt[] = {IA, SA, SA, IA, SA, DA, SA, IA, IA, IA, ps_supplycost, SA};
    Schema join_sch("join_sch", outAtts, joinatt);
    J.Use_n_Pages(1);
    J.Run(_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);

    char *str_sum = "(ps_supplycost)";
    get_cnf(str_sum, &join_sch, func);
    T.Run(_s_ps, _out, func);

    SF_ps.WaitUntilDone();
    J.WaitUntilDone();
    T.WaitUntilDone();

    Schema sum_sch("sum_sch", 1, &DA);

    Record tempRec;

    _out.Remove(&tempRec);

    string s = tempRec.ToString(&sum_sch);
    s.pop_back();

    dbf_s.Close();
    dbf_ps.Close();

    EXPECT_EQ(s, "381856.63");
}

TEST_F(RelOpTest, test5){ 

    DBFile dbf_s, dbf_ps;
    CNF cnf_s, cnf_ps, cnf_p_ps;
    Record lit_s, lit_ps, lit_p_ps;
    Schema s_schema("catalog", "supplier"), ps_schema("catalog", "partsupp");
    Pipe _s(100), _ps(100), _s_ps(100), _out(100);
    SelectFile SF_s, SF_ps;
    Join J;
    GroupBy G;
    Function func;

    char *pred_s = "(s_suppkey = s_suppkey)";
    get_cnf(pred_s, &s_schema, cnf_s, lit_s);
    dbf_s.Open("data/supplier.bin");
    SF_s.Run(dbf_s, _s, cnf_s, lit_s);

    char *pred_ps = "(ps_suppkey = ps_suppkey)";
    get_cnf(pred_ps, &ps_schema, cnf_ps, lit_ps);
    dbf_ps.Open("data/partsupp.bin");
    SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);

    get_cnf("(s_suppkey = ps_suppkey)", &s_schema, &ps_schema, cnf_p_ps, lit_p_ps);

    int outAtts = 7 + 5;
    Attribute s_nationkey = {"s_nationkey", Int};
    Attribute ps_supplycost = {"ps_supplycost", Double};
    Attribute joinatt[] = {IA, SA, SA, s_nationkey, SA, DA, SA, IA, IA, IA, ps_supplycost, SA};
    Schema join_sch("join_sch", outAtts, joinatt);
    J.Use_n_Pages(1);
    J.Run(_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);

    // GroupBy G;
    char *str_sum = "(ps_supplycost)";
    get_cnf(str_sum, &join_sch, func);
    char *pred_g = "(s_nationkey)";
    CNF groupby_cnf;
    Record literal2;
    get_cnf (pred_g, &join_sch, groupby_cnf, literal2);

    OrderMaker ordermaker, dummy;
    groupby_cnf.GetSortOrders(ordermaker, dummy);

    G.Use_n_Pages(1);
    G.Run(_s_ps, _out, ordermaker, func);

    SF_ps.WaitUntilDone();
    J.WaitUntilDone();
    G.WaitUntilDone();

    Schema sum_sch("sum_sch", 1, &DA);
    int cnt = clear_pipe(_out, &sum_sch, false);
    cout << "Query5 returned sum for " << cnt << " groups (expected 9 groups)\n";

    EXPECT_EQ(cnt, 9);
}







