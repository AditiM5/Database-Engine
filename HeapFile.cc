#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"

#include <stdlib.h>
#include <unistd.h>
#include <iostream>


using namespace std;

HeapFile::HeapFile() {
    currentPage = new(std::nothrow) Page();
}

int HeapFile::Create(const char *f_path, void *startup) {
    file = new File();
    file->Open(0, f_path);
    currPageNum = 0;
    return 1;
}

void HeapFile::Load(Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen(loadpath, "r");

    if (!tableFile) {
        cout << "ERROR : Cannot open file. EXIT !!!\n";
        exit(1);
    }

    Record temp;

    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        // check for page overflow
        if (!currentPage->Append(&temp)) {
            WriteCurrentPageToDisk();
            // empty the page out
            delete currentPage;
            currentPage = new(std::nothrow) Page();
            // append record to empty page
            currentPage->Append(&temp);
        }
    }

    WriteCurrentPageToDisk();
//    MoveFirst();
}

int HeapFile::Open(const char *f_path) {
    file = new File();
    file->Open(1, f_path);
    return 1;
}

void HeapFile::Add(Record *rec) {
    // Get last page in the file
    if (file->GetLength() != 0) {
        if (!currPageNum + 1 == file->GetLength()) {
            file->GetPage(currentPage, file->GetLength() - 2);
            currPageNum = file->GetLength() - 1;
        }
    }

    if (!currentPage->Append(rec)) {
        WriteCurrentPageToDisk();
        currPageNum++;
        // empty the page out
        delete currentPage;
        currentPage = new(std::nothrow) Page();
        // append record to empty page
        currentPage->Append(rec);
    }
    // set to false as we're always appending a record to a page
    currentPage->pageToDisk = false;
}

int HeapFile::GetNext(Record &fetchme) {
    WriteCurrentPageToDisk();

    if (!currentPage->GetFirst(&fetchme)) {
        // assuming the page is empty here so we move to the next page
        if (file->GetLength() > currPageNum + 2) {
            file->GetPage(currentPage, ++currPageNum);
            currentPage->GetFirst(&fetchme);
            return 1;
        } else {
            return 0;
        }
    }
    return 1;
}

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    Record temp;
    ComparisonEngine comp;
    while (1) {
        if (GetNext(temp)) {
            if (comp.Compare(&temp, &literal, &cnf)) {
                fetchme.Copy(&temp);
                return 1;
            }
        } else {
            return 0;
        }
    }
}
