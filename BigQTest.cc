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

struct params {
    Pipe* pipe;
    char *filename;
    char *schema;
};

extern struct AndList *final;

void *producer(void *args) {
    struct params *params = (struct params *)args;
    Pipe *pipe = params->pipe;
    FILE *tableFile = fopen(params->filename, "r");
    Record temp;
    Schema myschema("catalog", params->schema);
    int i = 0;

    while (temp.SuckNextRecord(&myschema, tableFile) == 1) {
        pipe->Insert(&temp);
        // cout << ++i << endl;
    }

    pipe->ShutDown();
    pthread_exit(NULL);
    return 0;
}

class BigQTest : public ::testing::Test {
protected:

    FILE *tableFile;
    DBFile *sortedFile;
    Record temp, sortedRec;
    Schema *myschema;
    OrderMaker *sortorder;
    Pipe *input, *output;
    ComparisonEngine ceng;
};

TEST_F(BigQTest, SortOrderTest) {
    tableFile = fopen("data/test.tbl", "r");
    myschema = new Schema("catalog", "lineitem");
    sortedFile = new DBFile();
    sortedFile->Open("data/lineitemtest.bin");
    sortedFile->MoveFirst();
    sortorder = new OrderMaker(myschema);
    input = new Pipe(100);
    output = new Pipe(100);

    int counter = 0;
    while (temp.SuckNextRecord(myschema, tableFile) == 1) {
        input->Insert(&temp);
        counter++;
    }

    input->ShutDown();

    BigQ *bq = new BigQ(*input, *output, *sortorder, 1);

    counter = 0;

    while (output->Remove(&temp) && sortedFile->GetNext(sortedRec)) {
        counter++;
        EXPECT_EQ(0, ceng.Compare(&temp, &sortedRec, sortorder));
    }
}

// File is larger than runlen
TEST_F(BigQTest, SortSingleAttTest){
    myschema = new Schema("catalog", "lineitem");
    sortorder = new OrderMaker;
    input = new Pipe(100);
    output = new Pipe(100);

    sortedFile = new DBFile();
    sortedFile->Open("data/lineitem.bin.bigq");
    sortedFile->MoveFirst();

    cout << "Enter CNF (l_shipdate) AND (l_extendedprice) AND  (l_quantity) (Press Ctrl+D when done)" << endl;
    if (yyparse() != 0) {
        cout << "Can't parse your sort CNF.\n";
        exit(1);
    }

    struct params *params = new struct params;
    params->pipe = input;
    params->filename = "data/lineitem.tbl";
    params->schema = "lineitem";

    Record literal;
    OrderMaker dummy;
    CNF sort_pred;
    sort_pred.GrowFromParseTree(final, myschema, literal); // constructs CNF predicate
    sort_pred.GetSortOrders(*sortorder, dummy);

    pthread_t thread1;

    pthread_create(&thread1, NULL, producer, (void *)params);

    BigQ *bq = new BigQ(*input, *output, *sortorder, 2);

    int counter = 0;

    while (output->Remove(&temp)&& sortedFile->GetNext(sortedRec)) {
        // cout << counter++ << endl; 
        EXPECT_EQ(0, ceng.Compare(&temp, &sortedRec, sortorder));
    }

    pthread_join(bq->threadID, NULL);
}

// more runs than filelen
TEST_F(BigQTest, SortMergeTest) {
    myschema = new Schema("catalog", "region");
    sortorder = new OrderMaker;
    input = new Pipe(100);
    output = new Pipe(100);

    sortedFile = new DBFile();
    sortedFile->Open("data/region.bin.bigq");
    sortedFile->MoveFirst();

    cout << "Enter CNF (r_name) (Press Ctrl+D when done)" << endl;
    if (yyparse() != 0) {
        cout << "Can't parse your sort CNF.\n";
        exit(1);
    }

    struct params *params = new struct params;
    params->pipe = input;
    params->filename = "data/region.tbl";
    params->schema = "region";

    Record literal;
    CNF sort_pred;
    OrderMaker dummy;
    sort_pred.GrowFromParseTree(final, myschema, literal);
    sort_pred.GetSortOrders(*sortorder, dummy);

    pthread_t thread1;

    pthread_create(&thread1, NULL, producer, (void *)params);

    BigQ *bq = new BigQ(*input, *output, *sortorder, 5);

    int counter = 0;

    while (output->Remove(&temp)&& sortedFile->GetNext(sortedRec)) {
        EXPECT_EQ(0, ceng.Compare(&temp, &sortedRec, sortorder));
    }

    pthread_join(thread1, NULL);
    pthread_join(bq->threadID, NULL);
}

