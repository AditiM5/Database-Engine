
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

    Schema myschema("catalog", "nation");

    DBFile newFile;
    newFile.Create("data/nation.bin", heap, NULL);
    newFile.Load(myschema, "data/nation.tbl");
    newFile.Close();
    return 0;
}




