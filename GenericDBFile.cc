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


void GenericDBFile::WriteOrderMaker(OrderMaker *sortOrder, FILE *file) {
    int numAtts = sortOrder->numAtts;
    fprintf(file, "%d", numAtts);
    int *whichAtts = sortOrder->whichAtts;
    Type *whichTypes = sortOrder->whichTypes;
    string temp;
    for (int i = 0; i < numAtts; i++) {
        temp = to_string(whichAtts[i]);
        switch (whichTypes[i]) {
            case Type::Int:
                temp = temp + " " + "Int";
                break;
            case Type::String:
                temp = temp + " " + "String";
                break;
            case Type::Double:
                temp = temp + " " + "Double";
                break;
        }
        const char *data = temp.c_str();
        fprintf(file, "%s", data);
    }
}

// first sentence of "sorted" already read
// here the next one, i.e, numAtts onwards will be read
void GenericDBFile::ReadOrderMaker(OrderMaker *sortOrder, FILE *file) {
    char space[100];
    // num Atts read
    fscanf(file, "%s", space);
    sortOrder->numAtts = stoi(space);

    for (int i = 0; i < sortOrder->numAtts; i++) {
        fscanf(file, "%s", space);
        sortOrder->whichAtts[i] = stoi(space);

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
