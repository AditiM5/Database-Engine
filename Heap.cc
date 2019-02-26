#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Heap.h"

#include <stdlib.h>
#include <unistd.h>
#include <iostream>


using namespace std;

Heap::Heap(){
    currentPage = new(std::nothrow) Page();

}

int Heap::Create(const char *f_path, void *startup) {
  file = new File();
  file->Open(0, f_path);
  const char *m_path = *f_path + ".data";
  FILE *metadata = fopen(m_path, "w");
  fprintf(metadata, "%s", "heap");
  fclose(metadata);
  currPageNum = 0;
  return 1;
}

void Heap::Load(Schema &f_schema, const char *loadpath) {
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

int Heap::Open(const char *f_path) {
    file = new File();
    file->Open(1, f_path);
    return 1;
}

void Heap::Add(Record *rec) {
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

int Heap::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    Record temp;
    ComparisonEngine comp;
    while (1) {
        if (GenericDBFile::GetNext(temp)) {
            if (comp.Compare(&temp, &literal, &cnf)) {
                fetchme.Copy(&temp);
                return 1;
            }
        } else {
            return 0;
        }
    }
}
