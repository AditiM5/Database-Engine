
#include <stdlib.h>
#include <iostream>
#include "Function.h"
#include "Record.h"
#include "Pipe.h"
#include "pthread.h"
#include "RelOp.h"

using namespace std;

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

extern "C" {
int yyparse(void);  // defined in y.tab.c
}

extern struct AndList *final;

void* producer1(void *args) {
    Pipe *in = (Pipe *)args;
    FILE *tableFile = fopen("data-1GB/nation.tbl", "r");

    Record temp;
    Schema mySchema("catalog", "nation");

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        in->Insert(&temp);
    }

    in->ShutDown();
}

void* producer2(void *args) {
    Pipe *in = (Pipe *)args;
    FILE *tableFile = fopen("data-1GB/region.tbl", "r");

    Record temp;
    Schema mySchema("catalog", "region");

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        in->Insert(&temp);
    }

    in->ShutDown();
}

int main() {
    //try to parse the CNF
    cout << "Enter in your CNF: ";
    if (yyparse() != 0) {
        cout << "Can't parse your CNF.\n";
        exit(1);
    }

    cout << "hello in main" << endl;
    // suck up the schema from the file
    Schema myschema1("catalog", "nation");
    Schema myschema2("catalog", "region");

    // grow the CNF expression from the parse tree
    CNF myComparison;
    Record literal;
    // myComparison.GrowFromParseTree(final, &myschema1, literal);
    myComparison.GrowFromParseTree(final, &myschema1, &myschema2, literal);
    Pipe inL(100), inR(100), out_sp(100), out_p(100), out_sf(100), out_j(100);


    cout << "hello 2 in main" << endl;
    int numAtts = 9;
    int keepMe[] = {0,1,7};
    int numAttsIn = numAtts;
	int numAttsOut = 3;

    Attribute atts[] = {IA, SA, DA};

    pthread_t thread1, thread2;

    pthread_create(&thread1, NULL, producer1, (void *)&inL);
    pthread_create(&thread2, NULL, producer2, (void *)&inR);

// cout << "hello 3 in main" << endl;
    // DBFile dbFile;
    // dbFile.Open("data-1GB/part.bin");
    // dbFile.MoveFirst();
    // SelectFile sf;
    // SelectPipe sp;
    Project p;
    Join j;

    Record tempRec;

    // sp.Run(in, out_sp, myComparison, literal);

    // sf.Run(dbFile, out_sf, myComparison, literal);
    // p.Run(out_sf, out_p, keepMe, numAttsIn, numAttsOut);
    cout << "Calling Join!!!" << endl;
    j.Use_n_Pages(5); 
    j.Run(inL, inR, out_j, myComparison, literal);

    int count = 0;
    // Schema out_sch ("out_sch", numAttsOut, atts);

    while(out_j.Remove(&tempRec)){
        // tempRec.Print(&myschema);
        count++;
    }

    cout << "Records fetched: " << count << endl;
    // dbFile.Close();
   
}
