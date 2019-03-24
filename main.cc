
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
    FILE *tableFile = fopen("data-1GB/supplier.tbl", "r");

    Record temp;
    Schema mySchema("catalog", "supplier");

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        in->Insert(&temp);
    }

    in->ShutDown();
    pthread_exit(NULL);
}

void* producer2(void *args) {
    Pipe *in = (Pipe *)args;
    FILE *tableFile = fopen("data-1GB/partsupp.tbl", "r");

    Record temp;
    Schema mySchema("catalog", "partsupp");

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        in->Insert(&temp);
    }

    in->ShutDown();
    pthread_exit(NULL);
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
    Schema myschema1("catalog", "supplier");
    Schema myschema2("catalog", "partsupp");

    // grow the CNF expression from the parse tree
    CNF myComparison;
    Record literal;
    // myComparison.GrowFromParseTree(final, &myschema1, literal);
    myComparison.GrowFromParseTree(final, &myschema1, &myschema2, literal);
    Pipe inL(100), inR(100), out_sp(100), out_p(100), out_sf(100), out_j(100);

    // OrderMaker left, right;
    // myComparison.GetSortOrders(left, right);


    cout << "hello 2 in main" << endl;
    // int numAtts = 9;
    // int keepMe[] = {0,1,7};
    // int numAttsIn = numAtts;
	// int numAttsOut = 3;

    // Attribute atts[] = {IA, SA, DA};

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

    // BigQ bigq(inL, out_j, left, 1);

    // sp.Run(in, out_sp, myComparison, literal);

    // sf.Run(dbFile, out_sf, myComparison, literal);
    // p.Run(out_sf, out_p, keepMe, numAttsIn, numAttsOut);
    // cout << "Calling Join!!!" << endl;
    j.Use_n_Pages(10);
    j.Run(inL, inR, out_j, myComparison, literal);
    // j.WaitUntilDone();

    int numAtts = 12;
    int keepMe[] = {0,1,2,3,4,5,6,7,8,9,10,11};
    int numAttsIn = numAtts;
	int numAttsOut = 12;

    Attribute atts[] = {IA, SA, SA, IA, SA, DA, SA, IA, IA, IA, DA, SA};

    Schema s("blah", 12, atts);

    int count = 0;
    // Schema out_sch ("catalog", "nation");
    // cout << "Before printing" << endl;

    while(out_j.Remove(&tempRec)){
        // tempRec.Print(&s);
        if (count % 1000 == 0) cout << count << endl;
        count++;
    }

    // out_j.ShutDown();
    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);

    cout << "Records fetched: " << count << endl;
    // dbFile.Close();
   
}
