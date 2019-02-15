
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include <pthread.h>


using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;
// void *producer(void *arg) {
//     Pipe *pipe = (Pipe *)arg;
//     for(int i = 0; i < 100; i++) {
//         temp.SuckNextRecord(&myschema, tableFile);
//         pipe.Insert(&temp);
//         cout << "Inserting record: " << i << endl;
//     }

// }

void *consumer(void *arg) {
    Pipe *pipe = (Pipe *)arg;
    Record temp;
    Schema myschema ("catalog", "lineitem");
    int count = 0;

    while(pipe->Remove(&temp)) {
        temp.Print(&myschema);
        count++;
    }

    cout << "Removed count: " << count << endl;
}

int main() {
    Pipe input (100);
    Pipe output (100);

    FILE *tableFile = fopen("data/test.tbl", "r");
    Schema myschema ("catalog", "lineitem");
    Record temp;
    Record literal;

    // cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
    // if (yyparse() != 0) {
    //     cout << "Can't parse your sort CNF.\n";
    //     exit (1);
    // }

    // CNF sort_pred;
    // sort_pred.GrowFromParseTree (final, &myschema, literal);
    OrderMaker sortorder(&myschema);
    // sort_pred.GetSortOrders (sortorder, dummy);

    pthread_t thread1;

    pthread_create(&thread1, NULL, consumer, (void *)&output);
    BigQ bq (input, output, sortorder, 1);
    int i = 0;

    while(temp.SuckNextRecord(&myschema, tableFile) == 1) {
        input.Insert(&temp);
        cout << "Inserting record: " << ++i << endl;
    }

    // cout << "Removing records..." << endl;
    input.ShutDown();
    pthread_join(thread1, NULL);

    return 0;
}




