#include "BigQ.h"
#include <pthread.h>

struct BigQParams {
    Pipe *in;
    Pipe *out;
    OrderMaker *sortorder;
    int runlen;

    Record *tempRec;
    File *file;
    Page *currentPage;
    int *currPageNum;

    BigQ *ref;
};


void* proxyFunction(void *args){
    BigQParams *params = (BigQParams *) args;
    void *fooPtr = params->ref;
    static_cast<BigQ*>(fooPtr)->Worker(args);
}


BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    currentPage = new(std::nothrow) Page();

    if (currentPage == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    currPageNum = new int;
    *currPageNum = 0;

    file = new File();
    file->Open(0, "tempSortFile.bin");

    // initialize current page number
    currPageNum = 0;


    ////////////////////

    BigQParams params;
    params.currentPage = currentPage;
    params.file = file;
    params.currPageNum = currPageNum;

    params.in = &in;
    params.out = &out;
    params.sortorder = &sortorder;
    params.runlen = runlen;

    params.ref = this;

    // thread to dump data into the input pipe (for BigQ's consumption)
    pthread_create(&threadID, NULL, proxyFunction, (void *) &params);

}

void BigQ::Worker(void *args) {
    // read data from in pipe sort them into runlen pages
    BigQParams *params = (BigQParams *) args;

    Pipe *in;
    Pipe *out;
    OrderMaker *sortorder;
    int runlen;

    Record *tempRec;
    File *file;
    Page *currentPage;
    int *currPageNum;

    currentPage = params->currentPage;
    file = params->file;
    currPageNum = params->currPageNum;

    in = params->in;
    out = params->out;
    sortorder = params->sortorder;
    runlen = params->runlen;

    tempRec = new(std::nothrow) Record();

    if (tempRec == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    while (in->Remove(tempRec) && *currPageNum < runlen) {
        if (!currentPage->Append(tempRec)) {
            // sort records before writing to disk
            SortRecords(currentPage, sortorder);
            // write current page to disk
            WriteCurrentPageToDisk();
            // increment page number
            (*currPageNum)++;
            // empty the page out
            delete currentPage;
            currentPage = new(std::nothrow) Page();
            // append record to empty page
            currentPage->Append(tempRec);
        }
    }

    // deal with the remaining records
    if (*currPageNum < runlen) {
        // sort and write records to disk
        SortRecords(currentPage, sortorder);
        WriteCurrentPageToDisk();
        // empty the page out
        delete currentPage;
        currentPage = new(std::nothrow) Page();
    }

    file->Close();

    // construct priority queue over sorted runs and dump sorted data
    // into the out pipe

    // finally shut down the out pipe
    out->ShutDown();

}

void BigQ::WriteCurrentPageToDisk() {
    if (!currentPage->pageToDisk) {
        if (!file->GetLength()) {
            file->AddPage(currentPage, file->GetLength());
        } else {
            file->AddPage(currentPage, file->GetLength() - 1);
        }
//        fsync(file->myFilDes);
        currentPage->pageToDisk = true;
    }
}

void BigQ::SortRecords(Page *page, OrderMaker *sortorder) {
    Record *records = new Record[page->numRecs];

    // copy records into the array
    for (int i = 0; i < page->numRecs; i++) {
        page->GetFirst(records + i);
    }

    // sort the records
    MergeSort(records, 0, page->numRecs, sortorder);

    // copy the records back into the page
    for (int i = 0; i < page->numRecs; i++) {
        page->Append(records + i);
    }

    // cleanup
    delete[] records;
}

void BigQ::MergeSort(Record records[], int start, int end, OrderMaker *sortorder) {
    if (start < end) {
        int mid = (start + end) / 2;

        MergeSort(records, start, mid, sortorder);
        MergeSort(records, mid + 1, end, sortorder);
        Merge(records, start, mid, end, sortorder);
    }
}

void BigQ::Merge(Record records[], int start, int mid, int end, OrderMaker *sortorder) {
    ComparisonEngine ceng;

    // create a temp array
    Record temp[end - start + 1];

    // crawlers for both intervals and for temp
    int i = start, j = mid + 1, k = 0;

    // traverse both arrays and in each iteration add smaller of both elements in temp
    while (i <= mid && j <= end) {
        if (ceng.Compare(records + i, records + j, sortorder) <= 0) {
            temp[k] = *(records + i);
            k++;
            i++;
        } else {
            temp[k] = *(records + j);
            k++;
            j++;
        }
    }

    // add elements left in the first interval
    while (i <= mid) {
        temp[k] = *(records + i);
        k++;
        i++;
    }

    // add elements left in the second interval
    while (j <= end) {
        temp[k] = *(records + j);
        k++;
        j++;
    }

    // copy temp to original interval
    for (i = start; i <= end; i += 1) {
        *(records + i) = temp[i - start];
    }
}

BigQ::~BigQ() {
}
