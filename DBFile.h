#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"

class DBFile {

    GenericDBFile *genericDBFile;

public:

    DBFile();

    int Create(const char *fpath, fType file_type, void *startup);

    // Opens bin files
    int Open(const char *fpath);

    int Close();

    // Loads a tbl file. Call Create() before this
    void Load(Schema &myschema, const char *loadpath);

    void MoveFirst();

    void Add(Record *addme);

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};

#endif
