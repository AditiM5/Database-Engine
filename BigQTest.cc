#include "gtest/gtest.h"
#include <iostream>
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Schema.h"
#include "Pipe.h"
#include "unistd.h"
#include "DBFile.h"
#include "pthread.h"

using namespace std;

extern "C" {
    int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

class BigQTest : public ::testing::Test {
protected:
    FILE *tableFile;
    DBFile *sortedFile;
    Record temp, sortedRec;
    Schema *myschema1, *myschema2;
    OrderMaker *sortorder1, *sortorder2;
    Pipe *input, *output;
    ComparisonEngine ceng;
};

TEST_F(BigQTest, SortOrderTest) {
    tableFile = fopen("data/test.tbl", "r");
    myschema1 = new Schema("catalog", "lineitem");
    sortedFile = new DBFile();
    sortedFile->Open("data/lineitemtest.bin");
    sortedFile->MoveFirst();
    sortorder1 = new OrderMaker(myschema1);
    input = new Pipe(100);
    output = new Pipe(100);

    int counter = 0;
    while (temp.SuckNextRecord(myschema1, tableFile) == 1) {
        input->Insert(&temp);
        counter++;
    }

    input->ShutDown();

    BigQ *bq = new BigQ(*input, *output, *sortorder1, 1);

    counter = 0;

    while (output->Remove(&temp) && sortedFile->GetNext(sortedRec)) {
        counter++;
        EXPECT_EQ(0, ceng.Compare(&temp, &sortedRec, sortorder1));
    }
}

TEST_F(BigQTest, SortSingleAttTest){
    tableFile = fopen("data/test.tbl", "r");
    myschema1 = new Schema("catalog", "lineitem");
    myschema2 = new Schema("catalog", "partsupp");
    sortorder1 = new OrderMaker;
    sortorder2 = new OrderMaker;
    input = new Pipe(100);
    output = new Pipe(100);

    sortedFile = new DBFile();
    sortedFile->Open("data/partkeytest.bin");
    sortedFile->MoveFirst();

    cout << "Enter CNF (Press Ctrl+D when done)" << endl;
    if (yyparse() != 0) {
        cout << "Can't parse your sort CNF.\n";
        exit(1);
    }

    Record literal;
    CNF sort_pred;
    sort_pred.GrowFromParseTree(final, myschema1, myschema2, literal); // constructs CNF predicate
    sort_pred.GetSortOrders(*sortorder1, *sortorder2);

    int counter = 0;
    while (temp.SuckNextRecord(myschema1, tableFile) == 1) {
        input->Insert(&temp);
        counter++;
    }
    input->ShutDown();
    BigQ *bq = new BigQ(*input, *output, *sortorder1, 1);

    counter = 0;

    while (output->Remove(&temp)&& sortedFile->GetNext(sortedRec)) {
        counter++;
        EXPECT_EQ(0, ceng.Compare(&temp, &sortedRec, sortorder1));
    }
}

void *producer(void *arg) {
    Pipe *pipe = (Pipe *) arg;
    FILE *tableFile = fopen("data/mergetest.tbl", "r");
    Record temp;
    Schema myschema("catalog", "lineitem");
    int i = 0;

    while (temp.SuckNextRecord(&myschema, tableFile) == 1) {
        pipe->Insert(&temp);
    }

    pipe->ShutDown();
    pthread_exit(NULL);
}

TEST_F(BigQTest, SortMergeTest) {
    myschema1 = new Schema("catalog", "lineitem");
    myschema2 = new Schema("catalog", "partsupp");
    sortorder1 = new OrderMaker;
    sortorder2 = new OrderMaker;
    input = new Pipe(100);
    output = new Pipe(100);

    sortedFile = new DBFile();
    sortedFile->Open("data/partkeymergetest.bin");
    sortedFile->MoveFirst();

    cout << "Enter CNF (Press Ctrl+D when done)" << endl;
    if (yyparse() != 0) {
        cout << "Can't parse your sort CNF.\n";
        exit(1);
    }

    Record literal;
    CNF sort_pred;
    sort_pred.GrowFromParseTree(final, myschema1, myschema2, literal);
    sort_pred.GetSortOrders(*sortorder1, *sortorder2);

    pthread_t thread1;

    pthread_create(&thread1, NULL, producer, (void *)input);

    BigQ *bq = new BigQ(*input, *output, *sortorder1, 3);

    int counter = 0;

    while (output->Remove(&temp)&& sortedFile->GetNext(sortedRec)) {
        counter++;
        EXPECT_EQ(0, ceng.Compare(&temp, &sortedRec, sortorder1));
    }

    pthread_join(thread1, NULL);
    pthread_join(bq->threadID, NULL);
}

