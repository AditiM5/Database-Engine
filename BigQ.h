#ifndef BIGQ_H
#define BIGQ_H

#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <stdlib.h>

using namespace std;

class BigQ {
    friend void* proxyFunction(void *fooPtr, void *args);

private:

    Record *records;

    void WritePageToDisk(File *file, Page *page);

    void SortRecords(Page *page, OrderMaker *sortorder);

    void MergeSort(Record *records, int start, int end, OrderMaker *sortorder);

    void Merge(Record *records, int start, int mid, int end, OrderMaker *sortorder);


public:

    pthread_t threadID;

    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);

    void* Worker(void *args);

    ~BigQ();
};

#endif
