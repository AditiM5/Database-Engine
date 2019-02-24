#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

class SortedFile : public GenericDBFile{

public:

    SortedFile();

    int Create(const char *fpath, void *startup);

    // Opens bin files
    int Open(const char *fpath);

    // Loads a tbl file. Call Create() before this
    void Load(Schema &myschema, const char *loadpath);

    void Add(Record *addme);

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};

#endif
