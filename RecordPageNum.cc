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

void RecordPageNum::setRunNumber(int runNum) {
    runNumber = runNum;
}

int RecordPageNum::getRunNumber() {
    return runNumber;
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
    Record *temp = tempRec->getRecord();
    pageNumber = tempRec->pageNumber;
    runNumber = tempRec->runNumber;
    setRecord(temp);
}

void RecordPageNum::Copy(RecordPageNum *tempRec) {
    Record *temp = tempRec->getRecord();
    if(rec == NULL){
        rec = new Record();
    }
    pageNumber = tempRec->pageNumber;
    runNumber = tempRec->runNumber;
    rec->Copy(temp);
}