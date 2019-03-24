#ifndef BUFFERFILE_H
#define BUFFERFILE_H

#include "File.h"

class BufferFile{

private: 
    Page *curr_pages;
    int buff_size;
    File *file;
    // zero based indexing
    int curr_pagenum = 0;
    // actual number of pages loaded
    // not zero based , starts from 1
    int num_pages = 0; 
    const char *fpath;

    int file_pagenum = 0;

public:

    BufferFile(int buff_size);

    // // return 1 on success
    // int Create(const char *fpath);

    // Opens bin files
    int Open(const char *fpath);

    int Close();

    // Loads a tbl file. Call Create() before this
    void Load(Schema &myschema, const char *loadpath);

    void MoveFirst();

    void Add(Record *addme);

    int GetNext(Record *fetchme);
}











#endif