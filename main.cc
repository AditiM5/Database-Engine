
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
//    Schema lineitem("catalog", "lineitem");
//    TwoWayList<Record> *myRecs = new(std::nothrow) TwoWayList<Record>();
//
//    char fileName[] = "data/test.tbl";
//    FILE *tableFile = fopen(fileName, "r");
//
//    int count = 0;
//    Record tempRec;
//
//    while (tempRec.SuckNextRecord(&lineitem, tableFile) == 1) {
//        myRecs->Insert(&tempRec);
//        cout << "LL: " << myRecs->LeftLength() << " RL: " << myRecs->RightLength() << endl;
//        count++;
//    }

//    cout << "LL: " << myRecs->LeftLength() << " RL: " << myRecs->RightLength() << endl;


//    Record *temp;
//    for (int i = 0; i < 10; ++i) {
//        temp = myRecs->list->current->data;
//        temp->Print(&lineitem);
//        myRecs->Advance();
//    }

//    Record delTemp;
//
//    for (int i = 0; i < 10; ++i) {
//        myRecs->Remove(&delTemp);
//        delTemp.Print(&lineitem);
//        cout << "LL: " << myRecs->LeftLength() << " RL: " << myRecs->RightLength() << endl;
//    }


//    myRecs->Advance();
//
//    cout << "LL: " << myRecs->LeftLength() << " RL: " << myRecs->RightLength() << endl;

//    temp = myRecs->list->current->data;
//    temp->Print(&lineitem);


//    return 0;


    DBFile dbFile;
    Schema lineitem("catalog", "lineitem");
    char fileName[] = "data/test2.tbl";

    char tempMain[] = "tempMain";
    // CREATE
    dbFile.Create(tempMain, heap, NULL);
    FILE *tableFile = fopen(fileName, "r");

    int count = 0;
    Record tempRec;

    // ADD
    while (tempRec.SuckNextRecord(&lineitem, tableFile) == 1) {
//        tempRec.Print(&lineitem);
        dbFile.Add(&tempRec);
        count++;
//        cout << "Writing rec: " << count << endl;
    }
    cout << "Count : " << count << endl;

    count = 0;
    Record tempRec2;
    while (dbFile.GetNext(&tempRec2)) {
        tempRec.Print(&lineitem);
        count++;
    }
    cout << "GetNext count: " << count << endl;

    return 0;

}


