#ifndef BIGQ_H
#define BIGQ_H

#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <stdlib.h>
#include <string.h>
#include <string>

using namespace std;

class BigQ {
    friend void* proxyFunction(void *fooPtr, void *args);

private:

    const char *tempFileName;

    Record *records;

    void WritePageToDisk(File *file, Page *page);

    void SortRecords(Page *page, OrderMaker *sortorder, int numPages);

    void MergeSort(Record *records, int start, int end, OrderMaker *sortorder);

    void Merge(Record *records, int start, int mid, int end, OrderMaker *sortorder);

    void KWayMerge(Page *pages, Pipe *out, int runNum, int runLen, OrderMaker *sortorder, File *file);

public:

    pthread_t threadID;

    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);

    void* Worker(void *args);

    ~BigQ();
};

#endif
