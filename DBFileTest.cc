#include <unistd.h>
#include <iostream>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Schema.h"
#include "gtest/gtest.h"

extern "C" {
int yyparse(void);  // defined in y.tab.c
}
extern struct AndList *final;

class DBFileTest : public ::testing::Test {
   protected:
    FILE *tableFile;
    DBFile *dbfile;
    Record temp;
    Schema *myschema = new Schema("catalog", "lineitem");
    const char *tbl_path = "data/lineitem.tbl";
    struct SortInfo *sortinfo;

    void ParseCNF();
};

void DBFileTest::ParseCNF() {
    Record literal;
    cout << "\n enter sort order CNF (l_partkey) (when done press enter and ctrl-D):\n\t";
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF.\n";
        exit(1);
    }
    CNF sort_pred;
    sort_pred.GrowFromParseTree(final, myschema, literal);  // constructs CNF predicate
    OrderMaker dummy, sortorder;
    sort_pred.GetSortOrders(sortorder, dummy);

    sortinfo = new SortInfo;
    sortinfo->myOrder = &sortorder;
    cout << "Enter the runlength: " << endl;
    cin >> sortinfo->runLength;
}

// load test for sorted dbfile
TEST_F(DBFileTest, DBFileLoad) {
    dbfile = new DBFile;
    cout << "DBFile name is: lineitem.bin " << endl;
    ParseCNF();
    dbfile->Create("lineitem.bin", sorted, (void *)sortinfo);
    cout << "Created the file successfully!" << endl;
    cout << " lineitem.tbl will be loaded " << endl;
    dbfile->Load(*myschema, tbl_path);
    EXPECT_EQ(1, dbfile->Close());
}

// open and sequencially scan the sorted file
TEST_F(DBFileTest, DBFileSeqScan) {
    dbfile = new DBFile;
    cout << "DBFile name is: lineitem.bin " << endl;
    dbfile->Open("lineitem.bin");
    dbfile->MoveFirst();

    int counter = 0;
    while (dbfile->GetNext(temp) == 1) {
        counter += 1;
        if (counter % 10000 == 0) {
            cout << counter << "\n";
        }
    }
    cout << " scanned " << counter << " recs \n";
    dbfile->Close();

    EXPECT_EQ(6005, counter);
}

// scan and filter sorted file
// pass the query CNF
TEST_F(DBFileTest, DBFileScanFilter) {
    cout << "Enter your CNF: (enter : (l_partkey = 200) AND (l_orderkey > 5000) )" << endl;
    if (yyparse() != 0) {
        exit(1);
    }
    CNF cnf;
    Record literal;
    dbfile = new DBFile;
    cnf.GrowFromParseTree(final, myschema, literal);
    dbfile->Open("lineitem.bin");
    dbfile->MoveFirst();

    int counter = 0;
    while (dbfile->GetNext(temp, cnf, literal) == 1) {
        counter += 1;
        if (counter % 10000 == 0) {
            cout << counter << "\n";
        }
    }
    cout << " scanned " << counter << " recs \n";
    dbfile->Close();
    EXPECT_EQ(5, counter);

    delete dbfile;
}

// // create a new file, writes to it and then scans the records
// // repeats the same again to ensure complete testing of interleaving operations
TEST_F(DBFileTest, DBFileInterleave) {
    dbfile = new DBFile;
    tableFile = fopen(tbl_path, "r");
    ParseCNF();
    dbfile->Create("lineitemwrite2.bin", sorted, (void *)sortinfo);
    int counter = 0;
    Record tempRec;

    fseek(tableFile, 0, SEEK_SET);

    while (tempRec.SuckNextRecord(myschema, tableFile) == 1) {
        dbfile->Add(&tempRec);
        counter++;
    }

    cout << "Written " << counter << " records" << endl;
    EXPECT_EQ(6005, counter);

    dbfile->Close();
    dbfile->Open("lineitemwrite2.bin");
    dbfile->MoveFirst();

    counter = 0;

    while (dbfile->GetNext(temp) == 1) {
        counter++;
    }

    cout << " scanned " << counter << " recs \n";
    EXPECT_EQ(6005, counter);
    fseek(tableFile, 0, SEEK_SET);

    // add records again
    counter = 0;

    while (tempRec.SuckNextRecord(myschema, tableFile) == 1) {
        dbfile->Add(&tempRec);
        counter++;
    }

    cout << "Added " << counter << " records again" << endl;
    EXPECT_EQ(6005, counter);

    //scanning again
    dbfile->MoveFirst();
    counter = 0;
    while (dbfile->GetNext(tempRec)) {
        counter++;
    }
    cout << "Scanned " << counter << " records" << endl;

    EXPECT_EQ(12010, counter);
    dbfile->Close();
    delete dbfile;
    unlink("lineitemwrite2.bin");
}

// adding records to sorted file
TEST_F(DBFileTest, DBFileWrite) {
    dbfile = new DBFile;
    tableFile = fopen(tbl_path, "r");
    ParseCNF();
    dbfile->Create("lineitemwrite.bin", sorted, (void *)sortinfo);
    int count = 0;
    Record tempRec;
    while (tempRec.SuckNextRecord(myschema, tableFile) == 1) {
        dbfile->Add(&tempRec);
        count++;
    }
    dbfile->Close();
    cout << "Written " << count << " records" << endl;
    delete dbfile;
    unlink("lineitemwrite.bin");
    EXPECT_EQ(6005, count);
}
