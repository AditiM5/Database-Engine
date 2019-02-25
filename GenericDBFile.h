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

    // BigQ *bigq;
    // bool readMode;
    // Pipe *input;
    // Pipe *output;
    // int runLength;
    // OrderMaker *sortOrder;

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

    void WriteOrderMaker(OrderMaker *sortOrder, FILE *file);

    void ReadOrderMaker(OrderMaker *sortOrder, FILE *file);
};

#endif
