
#include <iostream>
#include "QueryPlan.h"

using namespace std;

extern "C" {
int yyparse(void);  // defined in y.tab.c
}

int main() {
    cout << "\n Enter Query: ";
    yyparse();
    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;

    stats->Read(statFileName);

    Query q(stats);
    q.QueryPlan();

    return 0;
}
