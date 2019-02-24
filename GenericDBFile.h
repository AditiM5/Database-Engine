#ifndef GENERIC_DBFILE_H
#define GENERIC_DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {
    heap, sorted, tree
} fType;

class GenericDBFile {

protected:

    File *file;
    Page *currentPage;
    int currPageNum;
    void WriteCurrentPageToDisk();

public:

    GenericDBFile();

    virtual int Create(const char *fpath, void *startup) = 0;

    virtual void Add(Record *addme) = 0;

    virtual int GetNext(Record &fetchme) = 0;

    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;

    // Opens bin files
    virtual int Open(const char *fpath) = 0;

    // Loads a tbl file. Call Create() before this
    virtual void Load(Schema &myschema, const char *loadpath) = 0;

    int Close();

    void MoveFirst();

};

#endif
