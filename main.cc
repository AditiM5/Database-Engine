
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
    Schema lineitem ("catalog", "lineitem");
    char fileName[] = "data/lineitem.tbl";
    dbFile.Load(lineitem, fileName);
    dbFile.Close();

    char tempFileName[] = "temp";
    File file;
    file.Open(1, tempFileName);
    cout << file.GetLength() << endl;
    int count = 0;

    for (int i = 0; i < file.GetLength() - 1; ++i) {
        Page temp;
        Record tempRec;
        file.GetPage(&temp, i);

        while(temp.GetFirst(&tempRec, false)) {
            tempRec.Print(&lineitem);
            cout << endl;
        }
    }
    cout << count << endl;

    return 0;

//	// try to parse the CNF
//	cout << "Enter in your CNF: ";
//  	if (yyparse() != 0) {
//		cout << "Can't parse your CNF.\n";
//		exit (1);
//	}
//
//	// suck up the schema from the file
//	Schema lineitem ("catalog", "lineitem");
//
//	// grow the CNF expression from the parse tree
//	CNF myComparison;
//	Record literal;
//	myComparison.GrowFromParseTree (final, &lineitem, literal);
//
//	// print out the comparison to the screen
//	myComparison.Print ();
//
//	// now open up the text file and start procesing it
//        FILE *tableFile = fopen (/home/suraj/Projects/p1/P1/data/lineitem.tbl", "r");
//
//        Record temp;
//        Schema mySchema ("catalog", "lineitem");
//
//	//char *bits = literal.GetBits ();
//	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
//	//literal.Print (&supplier);
//
//        // read in all of the records from the text file and see if they match
//	// the CNF expression that was typed in
//	int counter = 0;
//	ComparisonEngine comp;
//        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
//		counter++;
//		if (counter % 10000 == 0) {
//			cerr << counter << "\n";
//		}
//
//		if (comp.Compare (&temp, &literal, &myComparison))
//                	temp.Print (&mySchema);
//
//        }

}

//int main() {
//    DBFile dbFile;
//    Schema lineitem("catalog", "lineitem");
//    const char tempMain[] = "tempMain";
//    dbFile.Create(tempMain, heap, NULL);
//    FILE *tableFile = fopen ("data/lineitem.tbl", "r");
//    Record tempRec;
//    while (tempRec.SuckNextRecord (&lineitem, tableFile) == 1) {
//        dbFile.Add(&tempRec);
//    }
//    dbFile.Close();
//    dbFile.Load(lineitem, tempMain);
//    return 0;
//}




