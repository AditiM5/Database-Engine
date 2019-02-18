
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include <pthread.h>
#include <unistd.h>

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

void *producer(void *arg) {
    Pipe *pipe = (Pipe *) arg;
    FILE *tableFile = fopen("data/test.tbl", "r");
    Record temp;
    Schema myschema("catalog", "lineitem");
    int i = 0;

    while (temp.SuckNextRecord(&myschema, tableFile) == 1) {
        pipe->Insert(&temp);
        // cout << "Inserting record: " << i++ << endl;
    }

    pipe->ShutDown();
    pthread_exit(NULL);
}

void *consumer(void *arg) {
    Pipe *pipe = (Pipe *) arg;
    Record temp;
    Schema myschema("catalog", "lineitem");
    int count = 0;

    DBFile dbFile;
    dbFile.Create("data/lineitemtest.bin", heap, NULL);

    while (pipe->Remove(&temp)) {
        temp.Print(&myschema);
        dbFile.Add(&temp);
    }

    cout << "Close file: " << dbFile.Close() << endl;

    DBFile newFile;
    newFile.Open("data/lineitemtest.bin");
    newFile.MoveFirst();

    cout << "DBFile recs" << endl;

    while(newFile.GetNext(temp)){
        temp.Print(&myschema);
    }

    newFile.Close();
    
    // cout << "Removed count: " << count << endl;
    pthread_exit(NULL);
}

int main() {
    Pipe input(100);
    Pipe output(100);

    Schema myschema("catalog", "lineitem");
    Record temp;
    OrderMaker sortorder(&myschema);

    // cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
    // if (yyparse() != 0) {
    //     cout << "Can't parse your sort CNF.\n";
    //     exit(1);
    // }

    // cout << " \n";
    // Record literal;
    // CNF sort_pred;
    // sort_pred.GrowFromParseTree(final, &myschema, literal); // constructs CNF predicate
    // OrderMaker dummy;
    // sort_pred.GetSortOrders(sortorder, dummy);

    pthread_t thread1;
    pthread_t thread2;

    pthread_create(&thread1, NULL, producer, (void *) &input);
    pthread_create(&thread2, NULL, consumer, (void *) &output);

    BigQ bq(input, output, sortorder, 1);


    // usleep(2000000);
    pthread_join(thread2, NULL);
    cout << "Consumer done" << endl;
    pthread_join(thread1, NULL);
    cout << "Producer done" << endl;
 
    pthread_join(bq.threadID, NULL);

    // exit(0);

    // cout << "ThreadID : " << bq.threadID << endl;

    return 0;
}




