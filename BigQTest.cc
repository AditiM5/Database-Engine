#include "gtest/gtest.h"
#include <iostream>
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Schema.h"
#include "Pipe.h"
#include "unistd.h"
#include "DBFile.h"

using namespace std;

class BigQTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        cout << "Setting up stuff..." << endl;
        tableFile = fopen("data/test.tbl", "r");
        myschema = new Schema("catalog", "lineitem");
        sortedFile = new DBFile();
        sortedFile->Open("data/lineitemtest.bin");
        sortedFile->MoveFirst();
        sortorder = new OrderMaker(myschema);
        input = new Pipe(100);
        output = new Pipe(100);
        ceng = new ComparisonEngine;

        int counter = 0;
        while (temp.SuckNextRecord(myschema, tableFile) == 1) {
            input->Insert(&temp);
            counter++;
        }

        cout << "Inserted " << counter << " records" << endl;

        input->ShutDown();

        BigQ *bq = new BigQ(*input, *output, *sortorder, 1);

        usleep(1000000);
    }

    FILE *tableFile;
    DBFile *sortedFile;
    Record temp, sortedRec;
    Schema *myschema;
    OrderMaker *sortorder;
    Pipe *input, *output;
    ComparisonEngine *ceng;
};

TEST_F(BigQTest, SortOrderTest) {
    cout << "Removing records" << endl;
    int counter = 0;

    while (output->Remove(&temp) && sortedFile->GetNext(sortedRec)) {
        counter++;
        EXPECT_EQ(0, ceng->Compare(&temp, &sortedRec, sortorder));
    }    
    cout << "Removed " << counter << " records" << endl;
}