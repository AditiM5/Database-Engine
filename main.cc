
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

// Iterative BinarySearch lookup - Faster
int BinarySearch(Record *recs, int start, int end, Record *key, OrderMaker *sortOrder) {
    int index = -1;
    while (start <= end) {
        int mid = start + (end - start) / 2;
        cout << "Start: " << start << endl;
        cout << "End: " << end << endl;
        cout << "Mid: " << mid << endl;
    
        ComparisonEngine ceng;

        if (ceng.Compare((recs + mid), key, sortOrder) == 0) {
            cout << "Found record at position: " << mid << endl;
            index = mid;
            end = mid - 1;
        } else if (ceng.Compare((recs + mid), key, sortOrder) < 0) {
            // if left is greater than right
            end = mid - 1;
        } else if (ceng.Compare((recs + mid), key, sortOrder) > 0) {
            // if left is less than right
            start = mid + 1;
        }
    }
    return index;
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
    // struct SortInfo *sortinfo = new SortInfo;
    // sortinfo->myOrder = &sortorder;
    // cout << "In main: " << endl;
    // sortorder.Print();
    // sortinfo->runLength = 3;

    // FILE *tableFile = fopen("data/lineitem.tbl", "r");
    Record tempRec;

    Record *myRecs = new Record[10];

    int count = 0;
    // cout << "Create" << endl;
    // newFile.Create("data/lineitem.bin", sorted, (void *)sortinfo);
    // newFile.Open("data/lineitem.bin");
    // newFile.MoveFirst();
    int i = 0;
    // cout << "First ADD: " << endl;

    // while (tempRec.SuckNextRecord(&myschema, tableFile) == 1 && i < 10) {
    //     // tempRec.Print(&myschema);
    //     // (myRecs + i)->Consume(&tempRec);
    //     newFile.Add(&tempRec);
    //     count++;
    //     i++;
    // }

    // newFile.Close();

    // cout << "Opend second: " << endl;
    // newFile.Open("data/lineitem.bin");
    // newFile.MoveFirst();

    // newFile.Close();

    // cout << "Open last and scan" << endl;
    newFile.Open("data/lineitem.bin");
    newFile.MoveFirst();

    i = 0;

    cout << "What is in the key: " << endl;
    literal.Print(&myschema);
    while (newFile.GetNext(tempRec) == 1 && i < 10) {
        tempRec.Print(&myschema);
        (myRecs + i)->Consume(&tempRec);
        // newFile.Add(&tempRec);
        count++;
        i++;
    }

    // i = 0;
    // while (newFile.GetNext(tempRec) == 1) {
    //     // cout << "In the loop humans...." << endl;

    //     i++;
    // }

    // cout << "Count: " << i << endl;
    // newFile.Close();

    int index = BinarySearch(myRecs, 0, 9, &literal, &sortorder);

    cout << "The bin search result: " << index << endl;

    // while (newFile.GetNext(tempRec) == 1) {
    //     tempRec.Print(&myschema);
    // }

    // newFile.GetNext(tempRec, sort_pred, literal);
    // tempRec.Print(&myschema);

    // newFile.Close();
    return 0;
}
