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
    vector<int> filledPages;
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

    // Pages needed to be sorted
    Page *pages = new Page[runlen + 1];

    //
    int currPageInRun = 0;

    Record *tempRec = new (std::nothrow) Record();
    File *file = new File();
    milliseconds ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    string tempFileString = "tempSortFile" + to_string(ms.count()) + ".bin";
    tempFileName = tempFileString.c_str();
    file->Open(0, tempFileName);
    int count = 0;

    while (true) {
        runNum++;
        currPageInRun = 0;

        if (!tempRec->IsRecordEmpty()) {
            if (!(pages + currPageInRun)->Append(tempRec)) cout << "Page full here!!!!!" << endl;
            count++;
        }

        while (in->Remove(tempRec)) {
            // count++;
            if (tempRec->IsRecordEmpty()) {
                cout << "TempRec is empty" << endl;
            }

            if ((pages + currPageInRun)->Append(tempRec) == 0) {
                if (currPageInRun + 1 < runlen) {
                    currPageInRun++;
                    count++;
                    (pages + currPageInRun)->Append(tempRec);
                } else {
                    break;
                }
            } else
                count++;
        }

        numRecs = 0;
        // currPageNum is zero based, so add 1!
        int tempFilledPage = SortRecords(pages, sortorder, currPageInRun + 1);
        filledPages.push_back(tempFilledPage);
        for (int i = 0; i < tempFilledPage; i++) {
            WritePageToDisk(file, pages + i);
            numRecs += (pages + i)->numRecords();
        }

        cout << "Recs written to disk: " << numRecs << endl;

        delete[] pages;
        pages = new Page[runlen + 1];

        if (in->isDone()) {
            break;
        }
    }

    // if ((pages + currPageInRun)->numRecs != 0) {
    //     int tempFilledPage = SortRecords(pages, sortorder, currPageInRun + 1);
    //     filledPages.push_back(tempFilledPage);
    //     for (int i = 0; i < tempFilledPage; i++) {
    //         WritePageToDisk(file, pages + i);
    //     }
    // }

    delete[] pages;

    KWayMerge(file, out, runNum, filledPages, sortorder);

    // test code
    Page *tempPage = new Page;
    int check = 0;
    for (int i = 0; i < file->GetLength() - 1; i++) {
        file->GetPage(tempPage, i);
        check = tempPage->numRecords();
        cout << "Num recs after k way: " << check << endl;
    }

    // cout << "Total number of records after k way merge: " << check << endl;

    // finally shut down the out pipe
    out->ShutDown();
    file->Close();
    unlink(tempFileName);
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

int BigQ::SortRecords(Page *pages, OrderMaker *sortorder, int numPages) {
    int numRecs = 0;

    for (int i = 0; i < numPages; i++) {
        numRecs += (pages + i)->numRecords();
    }

    Record *records = new Record[numRecs];

    int i = 0, j = 0;
    Record tempRec;

    while (i < numPages && j < numRecs) {
        if ((pages + i)->GetFirst(&tempRec) == 0) {
            i++;
        } else {
            (records + j)->Consume(&tempRec);
            j++;
        }
    }

    MergeSort(records, 0, numRecs - 1, sortorder);

    i = 0, j = 0;
    int count = 0;

    while (j < numRecs) {
        if ((pages + i)->Append(records + j) == 0) {
            i++;
            (pages + i)->Append(records + j);
            count++;
            j++;
        } else {
            count++;
            j++;
        }
    }

    cout << "Count in sort recs: " << count << endl;

    delete[] records;
    return i + 1;
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

void BigQ::KWayMerge(File *file, Pipe *out, int runNum, vector<int> runLen, OrderMaker *sortorder) {
    Page *pages = new Page[runNum];

    for (int i = 0; i < runLen.size(); i++) {
        cout << "runLen[i]: " << runLen[i] << endl;
    }

    PriorityQueue pq(sortorder, runNum);
    RecordPageNum *tempRec = new RecordPageNum;
    Record *temp = new Record;

    // get sorted pages
    int currPage = 0;
    for (int i = 0; i < runNum; i++) {
        cout << "currPage: " << currPage << endl;
        file->GetPage((pages + i), currPage);
        currPage = currPage + runLen[i];

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
            int tempRuns = 0;
            for (int i = 0; i < runNumber; i++) {
                tempRuns += runLen[i];
            }

            // see if we have any pages left in the run
            if ((pageNumber + 1) % runLen[runNumber] != 0 && ((tempRuns + pageNumber + 1) < file->GetLength() - 1)) {
                file->GetPage((pages + runNumber), (tempRuns + pageNumber + 1));
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

    cout << "After k-way loop count: " << count << endl;
}

BigQ::~BigQ() {
}
