#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <string.h>


using namespace std;

DBFile::DBFile() {
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    switch (f_type)
    {
        case heap:
            genericDBFile = new HeapFile();
            break;

        case sorted:
            genericDBFile = new SortedFile();
            break;
    
        default:
            break;
    }
    genericDBFile->Create(f_path, startup);
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    genericDBFile->Load(f_schema, loadpath);
}

int DBFile::Open(const char *f_path) {
    string metadataFileName(f_path);
    metadataFileName += ".data";
    FILE *metadata = fopen(metadataFileName.c_str(), "r");
    char arr[10];
    fscanf(metadata, "%s", arr);
    fclose(metadata);

    if(!strcmp(arr, "heap")){
        genericDBFile = new HeapFile();
    } else {
        genericDBFile = new SortedFile();
    }
    genericDBFile->Open(f_path);
}

void DBFile::MoveFirst() {
    genericDBFile->MoveFirst();
}

int DBFile::Close() {
    genericDBFile->Close();
}

void DBFile::Add(Record *rec) {
    genericDBFile->Add(rec);
}

int DBFile::GetNext(Record &fetchme) {
    genericDBFile->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    genericDBFile->GetNext(fetchme, cnf, literal);
}
