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
