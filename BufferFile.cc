#include "BufferFile.h"

BufferFile::BufferFile(int buff_size) {
    this->buff_size = buff_size;
    curr_pages = new Page[buff_size];
}

// int BufferFile::Create(const char *fpath) {
//     file = new File();
//     file->Open(0, fpath);
//     curr_pagenum = 0;
//     return 1;
// }

int BufferFile::Open(const char *fpath) {
    file = new File();
    file->Open(1, fpath);

    this->fpath = fpath;

    // starting from the first page
    curr_pagenum = 0;
    // get n pages
    int i = 0;
    while (i < buff_size) {
        if (file->GetLength() > i + 1) {
            file->GetPage((curr_pages + i), i);
            i++;
            num_pages++;
        } else {
            break;
        }
    }
}

int BufferFile::Close(){
    return file->Close();
}

void BufferFile::MoveFirst(){
    Close();
    Open(fpath);
}

int BufferFile::GetNext(Record *fetchme){
    if (!(curr_pages + curr_pagenum)->GetFirst(fetchme)) {
        // assuming the page is empty here so we move to the next page
        if (curr_pagenum < num_pages + 1) {
            (curr_pages + ++curr_pagenum)->GetFirst(fetchme);
            return 1;
        } else {
            return 0;
        }
    }
}