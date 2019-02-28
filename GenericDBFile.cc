// #include "TwoWayList.h"
// #include "Record.h"
// #include "Schema.h"
// #include "File.h"
// #include "Comparison.h"
// #include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "Defs.h"

#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <string.h>

using namespace std;
// using namespace string;

// GenericDBFile::GenericDBFile() {
//     // currentPage = new(std::nothrow) Page();
// }

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

void GenericDBFile::WriteOrderMaker(OrderMaker *sortOrder, FILE *file) {
    int numAtts = sortOrder->numAtts;
    fprintf(file, "%d\n", numAtts);
    int *whichAtts = sortOrder->whichAtts;
    Type *whichTypes = sortOrder->whichTypes;
    string temp;
    for (int i = 0; i < numAtts; i++) {
        temp = std::to_string(whichAtts[i]);
        switch (whichTypes[i]) {
            case Int:
                temp = temp + " " + "Int";
                break;
            case String:
                temp = temp + " " + "String";
                break;
            case Double:
                temp = temp + " " + "Double";
                break;
        }
        const char *data = temp.c_str();
        fprintf(file, "%s\n", data);
    }
}

// first sentence of "sorted" already read
// here the next one, i.e, numAtts onwards will be read
void GenericDBFile::ReadOrderMaker(OrderMaker *sortOrder, FILE *file) {
    char space[100];
    // num Atts read
    fscanf(file, "%s", space);
    sortOrder->numAtts = atoi(space);

    for (int i = 0; i < sortOrder->numAtts; i++) {
        fscanf(file, "%s", space);
        sortOrder->whichAtts[i] = atoi(space);

        fscanf(file, "%s", space);
        if (!strcmp(space, "Int")) {
            sortOrder->whichTypes[i] = Int;
        } else if (!strcmp(space, "Double")) {
            sortOrder->whichTypes[i] = Double;
        } else if (!strcmp(space, "String")) {
            sortOrder->whichTypes[i] = String;
        } else {
            cout << "Bad attribute type for "
                 << "\n";
            exit(1);
        }
    }
}

int GenericDBFile::GetNextRecord(Record *tempRec) {
    if (!currentPage->GetFirst(tempRec)) {
        // assuming the page is empty here so we move to the next page
        if (file->GetLength() > currPageNum + 2) {
            file->GetPage(currentPage, ++currPageNum);
            currentPage->GetFirst(tempRec);
            return 1;
        } else {
            return 0;
        }
    }
    return 1;
}
