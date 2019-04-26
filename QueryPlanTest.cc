#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include "QueryPlan.h"
#include "gtest/gtest.h"

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char *);
extern "C" {
int yyparse(void);
}

bool compareFiles(const std::string &p1, const std::string &p2) {
    std::ifstream f1(p1, std::ifstream::binary | std::ifstream::ate);
    std::ifstream f2(p2, std::ifstream::binary | std::ifstream::ate);

    if (f1.fail() || f2.fail()) {
        return false;  //file problem
    }

    if (f1.tellg() != f2.tellg()) {
        return false;  //size mismatch
    }

    //seek back to beginning and use std::equal to compare contents
    f1.seekg(0, std::ifstream::beg);
    f2.seekg(0, std::ifstream::beg);
    return std::equal(std::istreambuf_iterator<char>(f1.rdbuf()),
                      std::istreambuf_iterator<char>(),
                      std::istreambuf_iterator<char>(f2.rdbuf()));
}

class QueryPlanTest : public ::testing::Test {
   protected:
};

TEST_F(QueryPlanTest, query1) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query =
        "SELECT SUM (ps.ps_supplycost), s.s_suppkey \
                   FROM part AS p, supplier AS s, partsupp AS ps \
                   WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0) \
                   GROUP BY s.s_suppkey";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q1.txt"), true);
}

TEST_F(QueryPlanTest, query2) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT SUM (l.l_extendedprice * (1 - l.l_discount)), l.l_orderkey, o.o_orderdate, o.o_shippriority \
                   FROM customer AS c, orders AS o, lineitem AS l \
                   WHERE (c.c_mktsegment = 'BUILDING') AND (c.c_custkey = o.o_custkey) AND (l.l_orderkey = o.o_orderkey) AND (l.l_orderkey < 100 OR o.o_orderkey < 100) \
                   GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q2.txt"), true);
}

TEST_F(QueryPlanTest, query3) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT l.l_orderkey, l.l_partkey, l.l_suppkey \
                   FROM lineitem AS l \
                   WHERE (l.l_returnflag = 'R') AND (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL')";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q3.txt"), true);
}

TEST_F(QueryPlanTest, query4) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT DISTINCT c1.c_name, c1.c_address, c1.c_acctbal \
                   FROM customer AS c1, customer AS c2 \
                   WHERE (c1.c_nationkey = c2.c_nationkey) AND (c1.c_name ='Customer#000070919')";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q4.txt"), true);
}

TEST_F(QueryPlanTest, query5) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT SUM(l.l_discount) \
                   FROM customer AS c, orders AS o, lineitem AS l \
                   WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30.0) AND (l.l_discount < 0.03)";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q5.txt"), true);
}

TEST_F(QueryPlanTest, query6) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT l.l_orderkey \
                   FROM lineitem AS l \
                   WHERE (l.l_quantity > 30.0)";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q6.txt"), true);
}

TEST_F(QueryPlanTest, query7) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT DISTINCT c.c_name \
                   FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r \
                   WHERE (l.l_orderkey = o.o_orderkey) AND (o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey)";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q7.txt"), true);
}

TEST_F(QueryPlanTest, query8) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT l.l_discount \
                   FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r \
                   WHERE (l.l_orderkey = o.o_orderkey) AND (o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey) AND (r.r_regionkey = 1) AND (o.o_orderkey < 10000)";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q8.txt"), true);
}

TEST_F(QueryPlanTest, query9) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT SUM (l.l_discount) \
                   FROM customer AS c, orders AS o, lineitem AS l \
                   WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30.0) AND (l.l_discount < 0.03)";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q9.txt"), true);
}

TEST_F(QueryPlanTest, query10) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT SUM (l.l_extendedprice * l.l_discount) \
                   FROM lineitem AS l \
                   WHERE (l.l_discount<0.07) AND (l.l_quantity < 24.0)";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q10.txt"), true);
}

TEST_F(QueryPlanTest, query11) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT c1.c_name, c1.c_address \
                   FROM customer AS c1, customer AS c2 \
                   WHERE (c1.c_nationkey = c2.c_nationkey)";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q11.txt"), true);
}

TEST_F(QueryPlanTest, query12) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT ps.ps_supplycost, s.suppkey \
                   FROM supplier AS s, partsupp AS ps \
                   WHERE (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0)";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q12.txt"), true);
}

TEST_F(QueryPlanTest, query13) {
    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "SELECT n.n_name \
                   FROM nation AS n, region AS r \
                   WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_nationkey > 5)";

    yy_scan_string(query);
    yyparse();

    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);
    EXPECT_EQ(compareFiles("out.txt", "query_plan_op/q13.txt"), true);
}