
#include <fstream>
#include <iostream>
#include "QueryPlan.h"

using namespace std;
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char *);
extern "C" {
int yyparse(void);  // defined in y.tab.c
}

// these data structures hold the result of the parsing
extern struct FuncOperator *finalFunction;  // the aggregate function (NULL if no agg)
extern struct TableList *tables;            // the list of tables and aliases in the query
extern struct AndList *boolean;             // the predicate in the WHERE clause
extern struct NameList *groupingAtts;       // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect;       // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts;                    // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;                    // 1 if there is a DISTINCT in an aggregate query

int main() {
    std::ofstream out("query_plan_op/q13.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query =
        "SELECT n.n_name \
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

    return 0;
}
