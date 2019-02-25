#include "SortedFile.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

#include <stdlib.h>
#include <unistd.h>
#include <iostream>

using namespace std;

SortedFile::SortedFile() {
    currentPage = new (std::nothrow) Page();
}

// create  a metadata file
int SortedFile::Create(const char *f_path, void *startup) {
    struct SortInfo *sortinfo = (struct SortInfo *)startup;
    const char *m_path = *f_path + ".data";
    FILE *metadata = fopen(m_path, "w");
    fprintf(metadata, "%s", "sorted");
    WriteOrderMaker(sortinfo->myOrder, metadata);
    fclose(metadata);

    sortOrder = sortinfo->myOrder;
    runLength = sortinfo->runLength;

    file = new File();
    file->Open(0, f_path);
    return 1;
}

void SortedFile::Load(Schema &f_schema, const char *loadpath) {
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
    return 1;
}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return 1;
}

// write OrderMaker to MetaData file
// numatts etc etc made public
// void SortedFile::WriteOrderMaker(OrderMaker *sortOrder, FILE *file) {
//     int numAtts = sortOrder->numAtts;
//     fprintf(file, "%d", numAtts);
//     int *whichAtts = sortOrder->whichAtts;
//     Type *whichTypes = sortOrder->whichTypes;
//     string temp;
//     for (int i = 0; i < numAtts; i++) {
//         temp = to_string(whichAtts[i]);
//         switch (whichTypes[i]) {
//             case Type::Int:
//                 temp = temp + " " + "Int";
//                 break;
//             case Type::String:
//                 temp = temp + " " + "String";
//                 break;
//             case Type::Double:
//                 temp = temp + " " + "Double";
//                 break;
//         }
//         const char *data = temp.c_str();
//         fprintf(file, "%s", data);
//     }
// }

// // first sentence of "sorted" already read
// // here the next one, i.e, numAtts onwards will be read
// void SortedFile::ReadOrderMaker(OrderMaker *sortOrder, FILE *file) {
//     char space[100];
//     // num Atts read
//     fscanf(file, "%s", space);
//     sortOrder->numAtts = stoi(space);

//     for (int i = 0; i < sortOrder->numAtts; i++) {
//         fscanf(file, "%s", space);
//         sortOrder->whichAtts[i] = stoi(space);

//         fscanf(file, "%s", space);
//         if (!strcmp(space, "Int")) {
//             sortOrder->whichTypes[i] = Int;
//         } else if (!strcmp(space, "Double")) {
//             sortOrder->whichTypes[i] = Double;
//         } else if (!strcmp(space, "String")) {
//             sortOrder->whichTypes[i] = String;
//         } else {
//             cout << "Bad attribute type for "
//                  << "\n";
//             exit(1);
//         }
//     }
// }