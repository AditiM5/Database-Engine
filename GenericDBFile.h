#ifndef GENERIC_DBFILE_H
#define GENERIC_DBFILE_H

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"
#include "Pipe.h"
#include "BigQ.h"


struct SortInfo { OrderMaker *myOrder; int runLength = 0;};

typedef enum {
    heap,
    sorted,
    tree
} fType;

class GenericDBFile {
protected:
    File *file;
    Page *currentPage;
    int currPageNum;
    void WriteCurrentPageToDisk();

    BigQ *bigq = NULL;
    bool readMode = true;
    Pipe *input;
    Pipe *output;
    int runLength;
    OrderMaker *sortOrder;

    struct SortInfo *sortdata;

public:
    GenericDBFile(){};

    virtual int Create(const char *fpath, void *startup) = 0;

    virtual void Add(Record *addme) = 0;

    virtual int GetNext(Record &fetchme) = 0;

    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;

    // Opens bin files
    virtual int Open(const char *fpath) = 0;

    // Loads a tbl file. Call Create() before this
    virtual void Load(Schema &myschema, const char *loadpath) = 0;

    virtual int Close() = 0;

    virtual void MoveFirst() = 0;

    void WriteOrderMaker(OrderMaker *sortOrder, FILE *file);

    void ReadOrderMaker(OrderMaker *sortOrder, FILE *file);

    int GetNextRecord(Record *tempRec);
};

#endif
