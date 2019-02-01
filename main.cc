
#include <iostream>
#include "TwoWayList.cc"
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main() {
    Schema lineitem("catalog", "lineitem");
    TwoWayList <Record> *myRecs = new (std::nothrow) TwoWayList<Record>();

    char fileName[] = "data/test2.tbl";
    FILE *tableFile = fopen(fileName, "r");

    int count = 0;
    Record tempRec;

    while (tempRec.SuckNextRecord(&lineitem, tableFile) == 1) {
        myRecs->Insert(&tempRec);
        count++;
    }

    cout << "LL: " << myRecs->LeftLength() << " RL: " << myRecs->RightLength() << endl;


    Record *temp;
//    myRecs->MoveToStart();
    for (int i = 0; i < 100; ++i) {
        myRecs->Advance();
        temp = myRecs->list->current->data;
        temp->Print(&lineitem);
    }

//    myRecs->Advance();
//
//    cout << "LL: " << myRecs->LeftLength() << " RL: " << myRecs->RightLength() << endl;

//    temp = myRecs->list->current->data;
//    temp->Print(&lineitem);


    return 0;
}

//DBFile dbFile;
//Schema lineitem("catalog", "lineitem");
//char fileName[] = "data/test2.tbl";
////    dbFile.Load(lineitem, fileName);
////    dbFile.Close();
//
//char tempMain[] = "tempMain";
//dbFile.Create(tempMain, heap, NULL);
//FILE *tableFile = fopen(fileName, "r");
//
//int count = 0;
//Record tempRec;
//
//while (tempRec.SuckNextRecord(&lineitem, tableFile) == 1) {
////        tempRec.Print(&lineitem);
//dbFile.Add(&tempRec);
//count++;
////        cout << "Writ?ng rec: " << count << endl;
//}
//
//dbFile.MoveFirst();
//
//
////    fclose(tableFile);
////    tableFile = fopen(fileName, "r");
////    while (tempRec.SuckNextRecord(&lineitem, tableFile) == 1) {
//////        tempRec.Print(&lineitem);
////        dbFile.Add(&tempRec);
////        count++;
////    }
//
////    cout << "Count for ADD: " << count << endl;
//
//count = 0;
//while (dbFile.GetNext(&tempRec)) {
//tempRec.Print(&lineitem);
//count++;
//
//}
//
//cout << "GetNext count: " << count << endl;
//return 0;




