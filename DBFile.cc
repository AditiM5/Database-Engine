#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <unistd.h>
#include <iostream>

using namespace std;

DBFile::DBFile() {
    currentPage = new(std::nothrow) Page();
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    switch (f_type) {
        case heap:
            file = new File();
            file->Open(0, f_path);
            currPageNum = 0;
            return 1;
            break;

        default:
            cout << "ERROR : Unknown file type. EXIT !!!\n";
            exit(1);
    }
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen(loadpath, "r");

    if (!tableFile) {
        cout << "ERROR : Cannot open file. EXIT !!!\n";
        exit(1);
    }

    Record temp;
    Page *page = new Page();

    // Using a temp file to load data
    file = new File();
    file->Open(0, "temp");
    
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        // check for page overflow
        if (!currentPage->Append(&temp)) {
            // write the full page to file
            if (!file->GetLength()) {
                file->AddPage(currentPage, file->GetLength());
            } else {
                file->AddPage(currentPage, file->GetLength() - 1);
            }
            // empty the page out
            currentPage->EmptyItOut();
            // append record to empty page
            currentPage->Append(&temp);
        }
    }
    
    if(!currentPage->pageToDisk) {
        if (!file->GetLength()) {
            file->AddPage(currentPage, file->GetLength());
        } else {
            file->AddPage(currentPage, file->GetLength() - 1);
        }
        currentPage->pageToDisk = true;
    }

    // ensure data is written to disk
    fsync(file->myFilDes);
    MoveFirst();
}

int DBFile::Open(const char *f_path) {
    file = new File();
    file->Open(1, f_path);
    file->GetPage(currentPage, 0);
    currPageNum = 0;

    return 1;
}

void DBFile::MoveFirst() {
    if (!currentPage->pageToDisk) {
        if (!file->GetLength()) {
            file->AddPage(currentPage, file->GetLength());
        } else {
            file->AddPage(currentPage, file->GetLength() - 1);
        }
        fsync(file->myFilDes);
        currentPage->pageToDisk = true;
    }

    file->GetPage(currentPage, 0);
    currPageNum = 0;
}

int DBFile::Close() {
    // close() returns 0 on success!
    if (!currentPage->pageToDisk) {
        if (!file->GetLength()) {
            file->AddPage(currentPage, file->GetLength());
        } else {
            file->AddPage(currentPage, file->GetLength() - 1);
        }
        fsync(file->myFilDes);
        currentPage->pageToDisk = true;
    }
    
    if (!file->Close()) {
        return 1;
    } else {
        return 0;
    }
}

void DBFile::Add(Record *rec) {
    // Get last page in the file
    if (file->GetLength() != 0) {
        if (!currPageNum + 1 == file->GetLength()) {
            file->GetPage(currentPage, file->GetLength() - 2);
            currPageNum = file->GetLength() - 1;
        }
    }

    if (!currentPage->Append(rec)) {
        // write the full page to file
        if (!file->GetLength()) {
            file->AddPage(currentPage, file->GetLength());
        } else {
            file->AddPage(currentPage, file->GetLength() - 1);
        }
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

int DBFile::GetNext(Record *fetchme) {

    if (!currentPage->pageToDisk) {
        if (!file->GetLength()) {
            file->AddPage(currentPage, file->GetLength());
        } else {
            file->AddPage(currentPage, file->GetLength() - 1);
        }
        fsync(file->myFilDes);
        currentPage->pageToDisk = true;
    }

    if (!currentPage->GetFirst(fetchme)) {
        // assuming the page is empty here so we move to the next page
        if (file->GetLength() > currPageNum + 2) {
            file->GetPage(currentPage, ++currPageNum);
            currentPage->GetFirst(fetchme);
            return 1;
        } else {
            return 0;
        }
    }

    return 1;
}

int DBFile::GetNext(Record *fetchme, CNF &cnf, Record &literal) {
    Record temp;

    if (!currentPage->GetFirst(&temp)) {
        // assuming the page is empty here so we move to the next page
        if (file->GetLength() > currPageNum + 1) {
            file->GetPage(currentPage, ++currPageNum);
            currentPage->GetFirst(&temp);
        } else {
            cout << "ERROR : No more pages left to navigate to !!!\n";
            return 0;
        }
    }

    ComparisonEngine comp;
    if (comp.Compare(&temp, &literal, &cnf)) {
        fetchme->Copy(&temp);
        return 1;
    }
    return 0;
}
