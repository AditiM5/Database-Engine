
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
    cout << "Enter in your CNF: ";
    if (yyparse() != 0) {
        cout << "Can't parse your CNF.\n";
        exit (1);
    }
// suck up the schema from the file
    Schema lineitem ("catalog", "lineitem");

    // grow the CNF expression from the parse tree
    CNF myComparison;
    Record literal;
    myComparison.GrowFromParseTree (final, &lineitem, literal);

    // print out the comparison to the screen
    myComparison.Print ();

    DBFile dbFile;
//    Schema lineitem("catalog", "lineitem");
    char fileName[] = "data/lineitem.tbl";
    dbFile.Load(lineitem, fileName);
//    dbFile.Close();

    char tempMain[] = "tempMain";
//    dbFile.Create(tempMain, heap, NULL);
//    FILE *tableFile = fopen(fileName, "r");

    int count = 0;
    Record tempRec;
//
//    while (tempRec.SuckNextRecord(&lineitem, tableFile) == 1) {
////        tempRec.Print(&lineitem);
//        dbFile.Add(&tempRec);
//        count++;
////        cout << "Writ?ng rec: " << count << endl;
//    }
//    cout << "Recs written: " << count << endl;

    dbFile.MoveFirst();

    count = 0;
    while (dbFile.GetNext(&tempRec, myComparison, literal)) {
        if(tempRec.IsRecordEmpty()) {
            continue;
        }
        tempRec.Print(&lineitem);
        count++;
    }

    cout << "GetNext count: " << count << endl;

    return 0;
}




