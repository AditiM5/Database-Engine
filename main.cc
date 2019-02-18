
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include <pthread.h>
#include <unistd.h>
#include "FlexLexer.h"
#include <string.h>

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
int readInputForLexer( char *buffer, int *numBytesRead, int maxBytesToRead );
}

static int globalReadOffset;
static char *globalInputText = "(l_partkey = ps_partkey)";

extern struct AndList *final;

int readInputForLexer( char *buffer, int *numBytesRead, int maxBytesToRead ) {
    int numBytesToRead = maxBytesToRead;
    int bytesRemaining = strlen(globalInputText)-globalReadOffset;
    int i;
    if ( numBytesToRead > bytesRemaining ) { numBytesToRead = bytesRemaining; }
    for ( i = 0; i < numBytesToRead; i++ ) {
        buffer[i] = globalInputText[globalReadOffset+i];
    }
    *numBytesRead = numBytesToRead;
    globalReadOffset += numBytesToRead;
    return 0;
}

void *producer(void *arg) {
    Pipe *pipe = (Pipe *) arg;
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
    Pipe *pipe = (Pipe *) arg;
    Record temp;
    Schema myschema("catalog", "lineitem");
    int count = 0;

    DBFile dbFile;
    dbFile.Create("data/partkeymergetest.bin", heap, NULL);

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
    Pipe input(100);
    Pipe output(100);
    globalReadOffset = 0;

    Schema myschema("catalog", "lineitem");
    Schema myschema2("catalog", "partsupp");
    Record temp;
    OrderMaker sortorder1, sortorder2;

    // cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
    // yy_scan_buffer("l_partkey = ps_partkey");
    
    if (yyparse() != 0) {
        cout << "Can't parse your sort CNF.\n";
        exit(1);
    }

    cout << " \n";
    Record literal;
    CNF sort_pred;
    sort_pred.GrowFromParseTree(final, &myschema, &myschema2, literal); // constructs CNF predicate
    sort_pred.GetSortOrders(sortorder1, sortorder2);

    pthread_t thread1;
    pthread_t thread2;

    pthread_create(&thread1, NULL, producer, (void *) &input);
    pthread_create(&thread2, NULL, consumer, (void *) &output);

    BigQ bq(input, output, sortorder1, 3);


    // // usleep(2000000);
    pthread_join(thread2, NULL);
    cout << "Consumer done" << endl;
    pthread_join(thread1, NULL);
    cout << "Producer done" << endl;
 
    pthread_join(bq.threadID, NULL);

    // exit(0);

    cout << "Hello" << endl;

    return 0;
}




