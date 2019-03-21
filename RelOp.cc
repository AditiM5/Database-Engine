#include "RelOp.h"
#include "RelOpStructs.h"

void *SelectPipe::Worker(void *args) {
    SelectPipeParams *params = (SelectPipeParams *)args;

    Pipe *in = params->inPipe;
    Pipe *out = params->outPipe;
    Record *literal = params->literal;
    CNF *selOp = params->selOp;

    ComparisonEngine ceng;

    while (in->Remove(tempRec)) {
        if (ceng.Compare(tempRec, literal, selOp)) {
            out->Insert(tempRec);
        }
    }
    out->ShutDown();
}

void *SelectPipeProxyFunction(void *args) {
    SelectPipeParams *params;
    params = (SelectPipeParams *)args;
    void *fooPtr = params->ref;
    static_cast<SelectPipe *>(fooPtr)->Worker(args);
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectPipeParams *params = new SelectPipeParams;
    params->inPipe = &inPipe;
    params->outPipe = &outPipe;
    params->selOp = &selOp;
    params->ref = this;
    params->literal = &literal;

    pthread_create(&thread, NULL, SelectPipeProxyFunction, (void *)params);
}

void SelectPipe::WaitUntilDone() {
    pthread_join(thread, NULL);
}
void SelectPipe::Use_n_Pages(int n) {}

void *SelectFile::Worker(void *args) {
    SelectFileParams *params = (SelectFileParams *)args;

    DBFile *inFile = params->inFile;
    Pipe *out = params->outPipe;
    Record *literal = params->literal;
    CNF *selOp = params->selOp;

    ComparisonEngine ceng;

    while (inFile->GetNext(*tempRec)) {
        if (ceng.Compare(tempRec, literal, selOp)) {
            out->Insert(tempRec);
        }
    }
    out->ShutDown();
}

void *SelectFileProxyFunction(void *args) {
    SelectFileParams *params;
    params = (SelectFileParams *)args;
    void *fooPtr = params->ref;
    static_cast<SelectFile *>(fooPtr)->Worker(args);
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectFileParams *params = new SelectFileParams;
    params->inFile = &inFile;
    params->outPipe = &outPipe;
    params->selOp = &selOp;
    params->ref = this;
    params->literal = &literal;

    pthread_create(&thread, NULL, SelectFileProxyFunction, (void *)params);
}

void SelectFile::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {
}

void *ProjectProxyFunction(void *args) {
    SelectFileParams *params;
    params = (SelectFileParams *)args;
    void *fooPtr = params->ref;
    static_cast<SelectFile *>(fooPtr)->Worker(args);
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    ProjectParams *params = new ProjectParams;
    params->inPipe = &inPipe;
    params->outPipe = &outPipe;
    params->keepMe = keepMe;
    params->numAttsInput = numAttsInput;
    params->numAttsOutput = numAttsOutput;

    pthread_create(&thread, NULL, ProjectProxyFunction, (void *)params);
}

void *SelectFile::Worker(void *args) {
    ProjectParams *params = (ProjectParams *)args;

    Pipe *in = params->inPipe;
    Pipe *out = params->outPipe;
    int *keepMe = params->keepMe;
    int numAttsInput = params->numAttsInput;
    int numAttsOutput = params->numAttsOutput;

    ComparisonEngine ceng;

    while (in->Remove(tempRec)) {
        tempRec->Project(keepMe, numAttsOutput, numAttsInput);
        out->Insert(tempRec);
    }
    out->ShutDown();
}

void Project::WaitUntilDone() {
    pthread_join(thread, NULL);
}