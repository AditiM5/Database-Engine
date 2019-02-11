
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

int main() {
// suck up the schema from the file
    Schema lineitem ("catalog", "lineitem");
    DBFile dbFile;
    char fileName[] = "data/lineitem.tbl";
    dbFile.Create(tempMain, heap, NULL);
    FILE *tableFile = fopen(fileName, "r");

    int count = 0;
    Record tempRec;
    Pipe in = new Pipe(100);
    Pine out = new Pipe(100);
    while (tempRec.SuckNextRecord(&lineitem, tableFile) == 1) {
//        tempRec.Print(&lineitem);
//        dbFile.Add(&tempRec);
        in->Insert(&tempRec);
        count++;
//        cout << "Writ?ng rec: " << count << end l;
    }
    cout << "Recs written: " << count << endl;

    BigQ *tempBigQ = new BigQ(in, out, );
    pthread_join(&(tempBigQ->threadID), NULL);


//    count = 0;
//    while (dbFile.GetNext(tempRec)) {
////        tempRec.Print(&lineitem);
//        count++;
//    }
//
//    cout << "GetNext count: " << count << endl;
    return 0;
}




