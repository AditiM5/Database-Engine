#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"

#include <stdlib.h>
#include <unistd.h>
#include <iostream>


using namespace std;

SortedFile::SortedFile() {
    currentPage = new(std::nothrow) Page();
}

int SortedFile::Create(const char *f_path, void *startup) {
    return 1;
}

void SortedFile::Load(Schema &f_schema, const char *loadpath) {

}

int SortedFile::Open(const char *f_path) {
    return 1;
}

void SortedFile::Add(Record *rec) {

}

int SortedFile::GetNext(Record &fetchme) {
    return 1;
}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return 1;
}
