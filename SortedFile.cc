#include "SortedFile.h"
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include "GenericDBFile.h"

using namespace std;

SortedFile::SortedFile() {
    currentPage = new (std::nothrow) Page();
    tempRec = new Record();
}

// create  a metadata file
int SortedFile::Create(const char *f_path, void *startup) {
    // SortInfo *sortinfo;
    sortdata = (SortInfo *)startup;
    const char *m_path = *f_path + "QQQQQ.data";
    FILE *metadata = fopen("blah.data", "w");
    fprintf(metadata, "%s\n", "sorted");
    WriteOrderMaker(sortdata->myOrder, metadata);
    fclose(metadata);

    sortOrder = sortdata->myOrder;
    runLength = sortdata->runLength;

    // cout << "Runlen from CREATE: " << runLength;
    // cout << "\n";
    // cout << "SortOrder from CREATE: " << endl;
    // sortOrder->Print();

    file = new File();
    file->Open(0, f_path);
    return 1;
}

void SortedFile::Load(Schema &f_schema, const char *loadpath) {
    // cout << "Runlen from LOAD: " << sortdata->runLength;
    // cout << "\n";
    // cout << "SortOrder from LOAD: " << endl;
    // (sortdata->myOrder)->Print();
    if (bigq == NULL) {
        input = new Pipe(100);
        output = new Pipe(100);
        bigq = new BigQ(*input, *output, *sortOrder, runLength);
    }
    readMode = false;
    FILE *tableFile = fopen(loadpath, "r");
    if (!tableFile) {
        cout << "ERROR : Cannot open file. EXIT !!!\n";
        exit(1);
    }
    Record temp;
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        input->Insert(&temp);
    }
    // cout << "Runlen from Load: " << runLength;
    input->ShutDown();
    while (output->Remove(&temp)) {
        if (!currentPage->Append(&temp)) {
            WriteCurrentPageToDisk();
            // empty the page out
            delete currentPage;
            currentPage = new (std::nothrow) Page();
            // append record to empty page
            currentPage->Append(&temp);
        }
    }
    WriteCurrentPageToDisk();
}

int SortedFile::Open(const char *f_path) {
    return 1;
}

void SortedFile::Add(Record *rec) {
    if (bigq == NULL) {
        input = new Pipe(100);
        output = new Pipe(100);
        bigq = new BigQ(*input, *output, *sortOrder, runLength);
    }

    // change mode to writing
    readMode = false;
    input->Insert(rec);
}

int SortedFile::GetNext(Record &fetchme) {
    SwitchFromReadToWrite();
    return GetNextRecord(&fetchme);
}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return 1;
}

void SortedFile::SwitchFromReadToWrite() {
    if (!readMode) {
        input->ShutDown();
        output->Remove(tempRec);
        MoveFirst();
        ComparisonEngine ceng;
        Record *fromFile = new Record;
        while (GetNextRecord(fromFile) == 1) {
            if (ceng.Compare(tempRec, fromFile, sortOrder) > 0)
                break;
        }

        int tempCurrPageNum = currPageNum;
        Pipe *tempInput, *tempOutput;
        tempInput = new Pipe(100);
        tempOutput = new Pipe(100);
        BigQ *tempQ = new BigQ(*tempInput, *tempOutput, *sortOrder, 10);
        // move to the first record of the
        file->GetPage(currentPage, tempCurrPageNum);
        // currentPage->MoveToStartPage();
        currentPage->EmptyItOut();
        currPageNum = tempCurrPageNum;

        while (GetNextRecord(fromFile) == 1) {
            tempInput->Insert(fromFile);
        }

        while (output->Remove(tempRec)) {
            tempInput->Insert(tempRec);
        }

        tempInput->ShutDown();

        while (tempOutput->Remove(tempRec)) {
            if (!currentPage->Append(tempRec)) {
                WriteCurrentPageToDisk();
                currPageNum++;
                // empty the page out
                delete currentPage;
                currentPage = new (std::nothrow) Page();
                // append record to empty page
                currentPage->Append(tempRec);
            }
        }
        // now switch to read mode / switch out of write mode
        readMode = true;
    }
}

void SortedFile::MoveFirst() {
    cout << "Move First in SortedFile" << endl;
    SwitchFromReadToWrite();
    WriteCurrentPageToDisk();
    file->GetPage(currentPage, 0);
    currPageNum = 0;
}

int SortedFile::Close() {
    cout << "\n Close First in SortedFile";
    SwitchFromReadToWrite();
    WriteCurrentPageToDisk();

    fsync(file->myFilDes);
    // returns 1 on success
    if (!file->Close()) {
        return 1;
    } else {
        return 0;
    }
}