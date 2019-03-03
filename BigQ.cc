#include "BigQ.h"
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include "File.h"
#include "PriorityQueue.h"

using namespace std;
using namespace std::chrono;

struct BigQParams {
    Pipe *in;
    Pipe *out;
    OrderMaker *sortorder;
    int runlen = 0;
    BigQ *ref;
};

void *proxyFunction(void *args) {
    BigQParams *params;
    params = (BigQParams *)args;
    void *fooPtr = params->ref;
    static_cast<BigQ *>(fooPtr)->Worker(args);
}

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    BigQParams *params = new BigQParams;
    params->in = &in;
    params->out = &out;
    params->sortorder = &sortorder;
    params->runlen = runlen;
    params->ref = this;

    // thread to dump data into the input pipe (for BigQ's consumption)
    pthread_create(&threadID, NULL, proxyFunction, (void *)params);
}

void *BigQ::Worker(void *args) {
    long numRecs = 0;
    BigQParams *params;
    params = (BigQParams *)args;
    // number of runs
    int runNum = 0;

    Pipe *in = params->in;
    Pipe *out = params->out;
    OrderMaker *sortorder = params->sortorder;

    // number of records on each run to be sorted
    int runlen = params->runlen;
    int runOffsets[runlen];

    // Pages needed to be sorted
    Page *pages = new Page[runlen];

    int currPageNum = 0;

    Record *tempRec = new (std::nothrow) Record();
    File *file = new File();
    milliseconds ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    string tempFileString = "data/tempSortFile" + to_string(ms.count()) + ".bin";
    tempFileName = tempFileString.c_str();
    file->Open(0, tempFileName);
    int count = 0;

    while (true) {
        runNum++;
        currPageNum = 0;
        if (!tempRec->IsRecordEmpty()) {
            (pages + currPageNum)->Append(tempRec);
            count++;
        }

        
        while (currPageNum < runlen && in->Remove(tempRec)) {
            // count++;
            if(tempRec->IsRecordEmpty()){
                cout << "TempRec is empty" << endl;
            }
            if (!((pages + currPageNum)->Append(tempRec))) {
                if(currPageNum + 1 < runlen) {
                    currPageNum++;
                    count++;
                    (pages + currPageNum)->Append(tempRec);
                } else {
                    break;
                }
            } else count++;
            // numRecs++;
        }

        // currPageNum is zero based, so add 1!
        SortRecords(pages, sortorder, currPageNum + 1);
        for (int i = 0; i < currPageNum + 1; i++) {
            WritePageToDisk(file, pages + i);
        }

        // delete[] pages;
        pages = new Page[runlen];

        if (in->isDone()) {
            break;
        }
    }

    if((pages + currPageNum)->numRecs != 0) {
        SortRecords(pages, sortorder, currPageNum + 1);
        for (int i = 0; i < currPageNum + 1; i++) {
            WritePageToDisk(file, pages + i);
        }
    }

    delete[] pages;

    KWayMerge(file, out, runNum, runlen, sortorder);

    // finally shut down the out pipe
    out->ShutDown();
    file->Close();
    remove(tempFileName);
    pthread_exit(NULL);
    return 0;
}

void BigQ::WritePageToDisk(File *file, Page *page) {
    if (!page->pageToDisk) {
        if (!file->GetLength()) {
            file->AddPage(page, file->GetLength());
        } else {
            file->AddPage(page, file->GetLength() - 1);
        }
        page->pageToDisk = true;
    }
}

void BigQ::SortRecords(Page *pages, OrderMaker *sortorder, int numPages) {
    int numRecs = 0;
    for (int i = 0; i < numPages; i++) {
        numRecs = numRecs + (pages + i)->numRecords();
    }

    records = new (std::nothrow) Record[numRecs];

    if (records == NULL) {
        cout << "Error allocating memory for records" << endl;
        exit(1);
    }

    Record tempRec;

    for (int i = 0, k = 0; i < numPages && k < numRecs; i++) {
        int n = (pages + i)->numRecs;
        for (int j = 0; j < n; j++) {
            (pages + i)->GetFirst(records + k);
            k++;
        }
    }

    // sort the records
    MergeSort(records, 0, numRecs - 1, sortorder);

    for (int k = 0, i = 0; k < numRecs && i < numPages; k++) {
        if (!(pages + i)->Append(records + k)) {
            i++;
            (pages + i)->Append(records + k);
        }
    }

    // cleanup
    delete[] records;
}

void BigQ::MergeSort(Record *records, int start, int end, OrderMaker *sortorder) {
    if (start < end) {
        int mid = (start + end) / 2;

        MergeSort(records, start, mid, sortorder);
        MergeSort(records, mid + 1, end, sortorder);
        Merge(records, start, mid, end, sortorder);
    }
}

void BigQ::Merge(Record *records, int start, int mid, int end, OrderMaker *sortorder) {
    ComparisonEngine ceng;

    // create a temp array
    Record *temp = new Record[end - start + 1];

    // crawlers for both intervals and for temp
    int i = start, j = mid + 1, k = 0;

    // traverse both arrays and in each iteration add smaller of both elements in temp
    while (i <= mid && j <= end) {
        if (ceng.Compare(records + i, records + j, sortorder) <= 0) {
            (temp + k)->Consume(records + i);
            k++;
            i++;
        } else {
            (temp + k)->Consume(records + j);
            k++;
            j++;
        }
    }

    // add elements left in the first interval
    while (i <= mid) {
        (temp + k)->Consume(records + i);
        k++;
        i++;
    }

    // add elements left in the second interval
    while (j <= end) {
        (temp + k)->Consume(records + j);
        k++;
        j++;
    }

    // copy temp to original interval
    for (i = start; i <= end; i++) {
        (records + i)->Consume(temp + i - start);
    }

    delete[] temp;
}

void BigQ::KWayMerge(File *file, Pipe *out, int runNum, int runLen, OrderMaker *sortorder) {
    
    Page *pages = new Page[runNum];

    // get sorted pages
    for (int i = 0; i < runNum; i++) {
        file->GetPage((pages + i), i * runLen);
    }

    PriorityQueue pq(sortorder, runNum);
    RecordPageNum *tempRec = new RecordPageNum;
    Record *temp = new Record;

    for (int i = 0; i < runNum; i++) {
        (pages + i)->GetFirst(temp);
        tempRec->setRecord(temp);
        tempRec->setPageNumber(0);
        tempRec->setRunNumber(i);
        pq.push(tempRec);
    }
    
    int count = 0;

    while (!pq.empty()) {
        count++;
        pq.top(tempRec);
        pq.pop();
        int runNumber = tempRec->getRunNumber();
        int pageNumber = tempRec->getPageNumber();
        out->Insert(tempRec->getRecord());

        if (!((pages + runNumber)->GetFirst(temp))) {
            // see if we have any pages left in the run
            if ((pageNumber + 1) % runLen != 0 && ((runNumber * runLen + pageNumber + 1) < file->GetLength() - 1)) {
                file->GetPage((pages + runNumber), (runNumber * runLen + pageNumber + 1));
                (pages + runNumber)->GetFirst(temp);

                tempRec->setRecord(temp);
                tempRec->setRunNumber(runNumber);
                tempRec->setPageNumber(pageNumber + 1);
                pq.push(tempRec);
            } else
                continue;
        } else {
            tempRec->setRecord(temp);
            tempRec->setPageNumber(pageNumber);
            tempRec->setRunNumber(runNumber);
            pq.push(tempRec);
        }
    }
}

BigQ::~BigQ() {
}
