
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <iostream>
#include "BigQ.h"
#include "DBFile.h"
#include "FlexLexer.h"
#include "Pipe.h"
#include "Record.h"
#include "SortedFile.h"

using namespace std;

extern "C" {
int yyparse(void);  // defined in y.tab.c
int readInputForLexer(char *buffer, int *numBytesRead, int maxBytesToRead);
}

static int globalReadOffset;
static char *globalInputText = "(l_partkey = ps_partkey)";

extern struct AndList *final;

int readInputForLexer(char *buffer, int *numBytesRead, int maxBytesToRead) {
    int numBytesToRead = maxBytesToRead;
    int bytesRemaining = strlen(globalInputText) - globalReadOffset;
    int i;
    if (numBytesToRead > bytesRemaining) {
        numBytesToRead = bytesRemaining;
    }
    for (i = 0; i < numBytesToRead; i++) {
        buffer[i] = globalInputText[globalReadOffset + i];
    }
    *numBytesRead = numBytesToRead;
    globalReadOffset += numBytesToRead;
    return 0;
}

void *producer(void *arg) {
    Pipe *pipe = (Pipe *)arg;
    FILE *tableFile = fopen("data/mergetest.tbl", "r");
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
    Pipe *pipe = (Pipe *)arg;
    Record temp;
    Schema myschema("catalog", "lineitem");
    int count = 0;

    DBFile dbFile;
    dbFile.Create("data-1GB/lineitem.bin", heap, NULL);

    while (pipe->Remove(&temp)) {
        temp.Print(&myschema);
        dbFile.Add(&temp);
        count++;
    }

    cout << "Close file: " << dbFile.Close() << endl;
    cout << "Removed rec: " << count << endl;

    // DBFile newFile;
    // newFile.Open("data/partkeymergetest.bin");
    // newFile.MoveFirst();

    // cout << "DBFile recs" << endl;

    // while(newFile.GetNext(temp)){
    //     temp.Print(&myschema);
    // }

    // newFile.Close();

    // cout << "Removed count: " << count << endl;
    pthread_exit(NULL);
}

int main() {
    Schema myschema("catalog", "lineitem");

    cout << "Enter your CNF: (l_partkey)" << endl;
    yyparse();

    Record literal;
    CNF sort_pred;
    sort_pred.GrowFromParseTree(final, &myschema, literal);  // constructs CNF predicate
    OrderMaker dummy, sortorder;
    sort_pred.GetSortOrders(sortorder, dummy);

    DBFile newFile;
    struct SortInfo *sortinfo = new SortInfo;
    sortinfo->myOrder = &sortorder;
    cout << "In main: "<< endl;
    // sortorder.Print();
    sortinfo->runLength = 3;

    FILE *tableFile = fopen("data/test.tbl", "r");
    Record tempRec;

    // newFile.Create("data/lineitem.bin", sorted, (void *)sortinfo);
    newFile.Open("data/lineitem.bin");
    cout << "After create: "<< endl;
    // newFile.Load(myschema, "data/lineitem.tbl");

    int count = 0;

    // while(tempRec.SuckNextRecord(&myschema, tableFile) == 1){
    //     // tempRec.Print(&myschema);
    //     newFile.Add(&tempRec);
    //     count++;
    // }
    // cout << "Number Of Records read : Count: " << count << endl;

    count = 0;
    newFile.MoveFirst();
    while(newFile.GetNext(tempRec) == 1){
        count++;
        tempRec.Print(&myschema);
    }

    cout << "Sorted Count: " << count << endl;
    cout << "\n Closing...";
    newFile.Close();
    return 0;
}
