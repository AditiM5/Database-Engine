#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"
#include "Defs.h"

#include <stdlib.h>
#include <unistd.h>
#include <iostream>


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
    genericDBFile = new HeapFile();
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
