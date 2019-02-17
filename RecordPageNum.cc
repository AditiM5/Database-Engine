#include "RecordPageNum.h"
#include <iostream>

using namespace std;

RecordPageNum::RecordPageNum() {
    rec = new Record;
}

void RecordPageNum::setPageNumber(int pageNum) {
    pageNumber = pageNum;
}

int RecordPageNum::getPageNumber() {
    return pageNumber;
}


Record *RecordPageNum::getRecord() {
    return rec;
}

void RecordPageNum::setRecord(Record *tempRec) {
    if(rec == NULL)
        rec = new Record();
    rec->Consume(tempRec);
}

RecordPageNum::~RecordPageNum() {
    if (rec != NULL)
        delete rec;
}

void RecordPageNum::Consume(RecordPageNum *tempRec) {
    cout << "Consuming in RECORDPAGENUM" << endl;
    Record *temp = tempRec->getRecord();
    pageNumber = tempRec->pageNumber;
    setRecord(temp);
}

void RecordPageNum::Copy(RecordPageNum *tempRec) {
    Record *temp = tempRec->getRecord();
    if(rec == NULL){
        rec = new Record();
    }
    pageNumber = tempRec->pageNumber;
    rec->Copy(temp);
}