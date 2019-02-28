#include "gtest/gtest.h"
#include <iostream>
#include "ComparisonEngine.h"
#include "Schema.h"
#include "unistd.h"
#include "DBFile.h"


extern "C" {
int yyparse(void);   // defined in y.tab.c
}
extern struct AndList *final;

class DBFileTest : public ::testing::Test {
protected:

    FILE *tableFile;
    DBFile *dbfile;
    Record temp;
    Schema *myschema;
    const char *tbl_path = "data/lineitem.tbl";
};


TEST_F(DBFileTest, DBFileLoad){
    dbfile = new DBFile;
    myschema = new Schema("catalog", "lineitem");
    cout << "DBFile name is: lineitem.bin " << endl;
    dbfile->Create("lineitem.bin", heap, NULL);
    cout << "Created the file successfully!" << endl;
    cout << " lineitem.tbl will be loaded " << endl;
    dbfile->Load(*myschema, tbl_path);
    EXPECT_EQ(1, dbfile->Close());
}


TEST_F(DBFileTest, DBFileSeqScan){
    dbfile = new DBFile;
    myschema = new Schema("catalog", "lineitem");
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

TEST_F(DBFileTest, DBFileScanFilter){
    cout << "Enter your CNF: (enter : (l_orderkey > 45) AND (l_orderkey < 500) )"<< endl;
    if(yyparse()!=0){
        exit(1);
    }
    CNF cnf;
    Record literal;
    dbfile = new DBFile;
    myschema = new Schema("catalog", "lineitem");
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
    EXPECT_EQ(446, counter);
}

TEST_F(DBFileTest, DBFileWrite){
    dbfile = new DBFile;
    myschema = new Schema("catalog", "lineitem");
    tableFile = fopen(tbl_path, "r");
    dbfile->Create("lineitemwrite.bin", heap, NULL);
    int count = 0;
    Record tempRec;
    while (tempRec.SuckNextRecord(myschema, tableFile) == 1) {
        dbfile->Add(&tempRec);
        count++;
    }
    dbfile->Close();
    cout << "Written " << count << " records" << endl;
    EXPECT_EQ(6005, count);
}

TEST_F(DBFileTest, DBFileInterleave){
    dbfile = new DBFile;
    myschema = new Schema("catalog", "lineitem");
    tableFile = fopen(tbl_path, "r");
    dbfile->Create("lineitemwrite.bin", heap, NULL);
    int counter = 0;
    Record tempRec;
    while (tempRec.SuckNextRecord(myschema, tableFile) == 1) {
        dbfile->Add(&tempRec);
        counter++;
    }
    cout << "Written " << counter << " records" << endl;
    EXPECT_EQ(6005, counter);

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
}



