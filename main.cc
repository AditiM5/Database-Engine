
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main() {
    DBFile dbFile;
    Schema lineitem("catalog", "lineitem");
    char fileName[] = "data/test.tbl";
    dbFile.Load(lineitem, fileName);
//    dbFile.Close();

    char tempMain[] = "tempMain";
//    dbFile.Create(tempMain, heap, NULL);
    FILE *tableFile = fopen(fileName, "r");

    int count = 0;
    Record tempRec;

//    while (tempRec.SuckNextRecord(&lineitem, tableFile) == 1) {
////        tempRec.Print(&lineitem);
//        dbFile.Add(&tempRec);
//        count++;
////        cout << "Writ?ng rec: " << count << endl;
//    }
//    cout << "Recs written: " << count << endl;

//    dbFile.MoveFirst();

    count = 0;
    while (dbFile.GetNext(&tempRec)) {
        tempRec.Print(&lineitem);
//        cout << ++count << endl;
        count++;
    }

//    cout << "GetNext count: " << count << endl;

//
//    count = 0;
//
//    fclose(tableFile);
//    tableFile = fopen(fileName, "r");
//    while (tempRec.SuckNextRecord(&lineitem, tableFile) == 1) {
////        tempRec.Print(&lineitem);
//        dbFile.Add(&tempRec);
//        count++;
//    }
//
//    dbFile.MoveFirst();
//
//    cout << "Count for ADD: " << count << endl;
//
//    count = 0;
//    while (dbFile.GetNext(&tempRec)) {
////        tempRec.Print(&lineitem);
//        count++;
//    }
//
//    cout << "GetNext count: " << count << endl;


    return 0;
}




