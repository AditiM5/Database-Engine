#include "QueryPlan.h"
#include "gtest/gtest.h"
#include <iostream>
#include <fstream>
#include <string>

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char *);
extern "C" {
int yyparse(void);
}

class QueryPlanTest : public ::testing::Test {
   protected:
};

TEST_F(QueryPlanTest, query0) {

    std::ofstream out("out.txt");
    std::streambuf *coutbuf = std::cout.rdbuf(); //save old buf
    std::cout.rdbuf(out.rdbuf()); 

    char *query = "SELECT SUM (ps.ps_supplycost), s.s_suppkey \
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

    EXPECT_EQ(0, 0);
}
