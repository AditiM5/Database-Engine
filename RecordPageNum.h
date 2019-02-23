

#ifndef DBI_PROJECT_RECORDPAGENUM_H
#define DBI_PROJECT_RECORDPAGENUM_H

#include "Record.h"

class RecordPageNum {

private:

    int pageNumber;

    int runNumber;

    Record *rec;
public:

    void setPageNumber(int pageNum);

    int getPageNumber();

    void setRunNumber(int runNum);

    int getRunNumber();

    Record* getRecord();

    void setRecord(Record *tempRec);

    void Consume(RecordPageNum *tempRec);

    void Copy(RecordPageNum *tempRec);

    ~RecordPageNum();

    RecordPageNum();
};


#endif //DBI_PROJECT_RECORDPAGENUM_H
