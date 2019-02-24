#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "Defs.h"

#include <stdlib.h>
#include <unistd.h>
#include <iostream>


using namespace std;

GenericDBFile::GenericDBFile() {
    // currentPage = new(std::nothrow) Page();

}

void GenericDBFile::MoveFirst() {
    WriteCurrentPageToDisk();

    file->GetPage(currentPage, 0);
    currPageNum = 0;
}

int GenericDBFile::Close() {
    WriteCurrentPageToDisk();
    fsync(file->myFilDes);
    // returns 1 on success
    if (!file->Close()) {
        return 1;
    } else {
        return 0;
    }
}

void GenericDBFile::WriteCurrentPageToDisk() {
    if (!currentPage->pageToDisk) {
        if (!file->GetLength()) {
            file->AddPage(currentPage, file->GetLength());
        } else {
            file->AddPage(currentPage, file->GetLength() - 1);
        }
        currentPage->pageToDisk = true;
    }
}
