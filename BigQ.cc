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
    // Page *currentPage;
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
    // params->currentPage = new Page();

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

    // number of records on each page!
    int currPageNum = 0;

    Record *tempRec = new (std::nothrow) Record();
    File *file = new File();
    milliseconds ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    string tempFileString = "data/tempSortFile" + to_string(ms.count()) + ".bin";
    tempFileName = tempFileString.c_str();
    file->Open(0, tempFileName);
    // Page *currentPage = new(std::nothrow) Page();
    // Page *currentPage;
    // currentPage = params->currentPage;
    int count = 0;
    // currPageNum = 0;

    while (true) {
        runNum++;
        currPageNum = 0;
        // usleep(1000000);
        // int inputEmpty = 0;
        // there is a next record in the file continue with runs
        if (!tempRec->IsRecordEmpty()) {
            (pages + currPageNum)->Append(tempRec);
            // count++;
        }

        while (in->Remove(tempRec) && currPageNum < runlen) {
            count++;
            if (!(pages + currPageNum)->Append(tempRec)) {
                // sort records before writing to disk
                // SortRecords(currentPage, sortorder);
                // write current page to disk
                // WritePageToDisk(file, currentPage);
                // increment page number
                currPageNum++;
                // empty the page out
                // delete currentPage;
                // currentPage = new (std::nothrow) Page();
                // append record to empty page
                // (pages + currPageNum)->Append(tempRec);
            }
            numRecs++;
        }

        // if(currPageNum >= runlen){
        //     runNum++;
        // }

        SortRecords(pages, sortorder, currPageNum);
        for (int i = 0; i < currPageNum; i++) {
            WritePageToDisk(file, pages + i);
        }

        delete[] pages;
        pages = new Page[runlen];

        if (in->isDone()) {
            break;
        }
    }

    cout << "Number of times removed from pipe: " << count << endl;

    // deal with the remaining records
    // if ((pages + currPageNum)->numRecs != 0) {
    //     // sort and write records to disk
    //     cout << "CurrentPage Number: " << currPageNum << endl;
    //     if (!(currPageNum < runlen)) {
    //         runNum++;
    //         cout << "Incrementing runNum" << endl;
    //         currPageNum = 0;
    //     }
    //     cout << "Run Num: " << runNum << endl;
    //     SortRecords(currentPage, sortorder);
    //     WritePageToDisk(file, currentPage);
    //     // increment page number
    //     currPageNum++;
    //     // empty the page out
    //     delete currentPage;
    //     // currentPage->EmptyItOut();
    //     currentPage = new (std::nothrow) Page();
    // }

    delete[] pages;
    pages = new Page[runNum];

    cout << "The runNum: " << runNum << endl;
    // get sorted pages
    for (int i = 0; i < runNum; i++) {
        cout << "i: " << i << endl;
        file->GetPage((pages + i), i * runlen);
    }

    KWayMerge(pages, out, runNum, runlen, sortorder, file);

    delete[] pages;

    // finally shut down the out pipe
    out->ShutDown();
    file->Close();
    // remove(tempFileName);
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
        // fsync(file->myFilDes);
        page->pageToDisk = true;
    }
}

void BigQ::SortRecords(Page *pages, OrderMaker *sortorder, int numPages) {
    int numRecs = 0;
    for (int i = 0; i < numPages; i++) {
        numRecs = numRecs + (pages + i)->numRecords();
    }
    records = new Record[numRecs];

    if (records == NULL) {
        cout << "Error allocating memory for records" << endl;
        exit(1);
    }

    int k = 0;

    for (int i = 0; i < numPages; i++) {
        for (int j = 0; j < (pages + i)->numRecords(); j++) {
            (pages + i)->GetFirst(records + k);
            k++;
        }
    }

    // sort the records
    MergeSort(records, 0, numRecs - 1, sortorder);

    // // copy the records back into the page
    // for (int i = 0; i < numRecs; i++) {
    //     page->Append(records + i);
    // }

    k = 0;
    for (int i = 0; i < numRecs; i++) {
        if (!(pages + k)->Append(records + i)) {
            k++;
            (pages + k)->Append(records + i);
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

void BigQ::KWayMerge(Page *pages, Pipe *out, int runNum, int runLen, OrderMaker *sortorder, File *file) {
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
        Schema myschema("catalog", "lineitem");
        tempRec->getRecord()->Print(&myschema);
        out->Insert(tempRec->getRecord());

        // (pageNumber+1 < file->GetLength() - 1)
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

    cout << "K way merge loop ran : " << count << " times" << endl;
}

BigQ::~BigQ() {
}
