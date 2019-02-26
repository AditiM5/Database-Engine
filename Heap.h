#ifndef HEAP_H
#define HEAP_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

// typedef enum {
//     heap, sorted, tree
// } fType;

// stub DBFile header..replace it with your own DBFile.h 

class Heap : virtual public GenericDBFile {

public:

    Heap();

    virtual int Create(const char *fpath, void *startup);

    // Opens bin files
    virtual int Open(const char *fpath);

    // Loads a tbl file. Call Create() before this
    virtual void Load(Schema &myschema, const char *loadpath);

    virtual void Add(Record *addme);

    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal);

    virtual ~Heap() = default;

};

#endif
