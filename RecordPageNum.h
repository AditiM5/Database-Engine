

#ifndef DBI_PROJECT_RECORDPAGENUM_H
#define DBI_PROJECT_RECORDPAGENUM_H

#include "Record.h"

class RecordPageNum {

private:

    int pageNumber;

    Record *rec;
public:

    void setPageNumber(int pageNum);

    int getPageNumber();

    Record* getRecord();

    void setRecord(Record *tempRec);

    void Consume(RecordPageNum *tempRec);

    void Copy(RecordPageNum *tempRec);

    ~RecordPageNum();
};


#endif //DBI_PROJECT_RECORDPAGENUM_H
