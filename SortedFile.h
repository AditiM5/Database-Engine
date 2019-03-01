#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "BigQ.h"
#include "GenericDBFile.h"
#include "Pipe.h"

// struct SortInfo { OrderMaker *myOrder; int runLength = 0;};

class SortedFile : public GenericDBFile {
    // BigQ *bigq;
    // bool readMode;
    // Pipe *input;
    // Pipe *output;
    // int runLength;
    // OrderMaker *sortOrder;

    // struct SortInfo *sortdata;

    // void WriteOrderMaker(OrderMaker *sortOrder, FILE *file);

    // void ReadOrderMaker(OrderMaker *sortOrder, FILE *file);

    Record *tempRec;

    bool prevGetNext = false;

    void SwitchFromReadToWrite();

    void WritePipeToDisk(Pipe *output);

    void initQ();

    int BinarySearch(Record *recs, int start, int end, Record *key, OrderMaker *sortOrder);

    bool CompareOrderMakerCNF(Record *record, Record *literal, OrderMaker *sortOrder, CNF *cnf);

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

    void MoveFirst();

    int Close();
};

#endif
