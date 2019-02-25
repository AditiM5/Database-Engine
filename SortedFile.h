#ifndef SORTEDFILE_H
#define SORTEDFILE_H

// #include "TwoWayList.h"
// #include "Record.h"
// #include "Schema.h"
// #include "File.h"
// #include "Comparison.h"
// #include "ComparisonEngine.h"
#include "GenericDBFile.h"
// #include "BigQ.h"
// #include "Pipe.h"

struct SortInfo { OrderMaker *myOrder; int runLength;};

class SortedFile : public GenericDBFile{

    BigQ *bigq;
    bool readMode;
    Pipe *input;
    Pipe *output;
    int runLength;
    OrderMaker *sortOrder;

    // void WriteOrderMaker(OrderMaker *sortOrder, FILE *file);

    // void ReadOrderMaker(OrderMaker *sortOrder, FILE *file);
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
