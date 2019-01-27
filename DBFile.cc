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

DBFile::DBFile () {

}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    switch(f_type) {
        case heap:
            file = new File();
            file->Open(0, f_path);
            return 1;
        break;

        default:
            cout << "ERROR : Unknown file type. EXIT !!!\n";
            exit(1);
    }
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen (loadpath, "r");

    if (!tableFile) {
        cout << "ERROR : Cannot open file. EXIT !!!\n";
        exit(1);
    }

    Record temp;
    Page page;

    // Using a temp file to load data
    file = new File();
    file->Open(0, "temp");

    int counter = 1;

    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {

        cout << "Appending record " << counter++ << endl;
        // check for page overflow
        if (!page.Append(&temp)) {
            // write the full page to file
            file->AddPage(&page, file->GetLength());
            // empty the page out
            page.EmptyItOut();
            // append record to empty page
            page.Append(&temp);
            cout << "Appending record " << counter++ << endl;
        }
    }

    // add last page to the file!!!
    file->AddPage(&page, file->GetLength());

    // ensure data is written to disk
    fsync(file->myFilDes);
    // move the pointer to the beginning??

}

int DBFile::Open (const char *f_path) {
    file = new File();
    file->Open(1, f_path);
    file->GetPage(&currentPage, 0);

    if (!currentPage->GetFirst(&currentRecord, false)) {
        cout << "ERROR : Page has no records. EXIT !!!\n";
        exit(1);
    }

    return 1;
}

void DBFile::MoveFirst () {
    file->GetPage(&currentPage, 0);

    if (!currentPage->GetFirst(&currentRecord, false)) {
        cout << "ERROR : Page has no records. EXIT !!!\n";
        exit(1);
    }
}

int DBFile::Close () {
    // close() returns 0 on success!
     if (!file->Close()) {
         return 1;
     } else {
         return 0;
     }
}

void DBFile::Add (Record &rec) {
    // Get last page in the file
    file->GetPage(&currentPage, file->GetLength());

    if (!currentPage->Append(rec)) {
        // write the full page to file
        file->AddPage(&currentPage, file->GetLength());
        // empty the page out
        currentPage->EmptyItOut();
        // append record to empty page
        currentPage->Append(&temp);
    }

    file->AddPage(&currentPage, file->GetLength());

    currentRecord = rec;
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
