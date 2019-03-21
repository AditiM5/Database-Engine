#ifndef REL_OP_H
#define REL_OP_H

#include "DBFile.h"
#include "Function.h"
#include "Pipe.h"
#include "Record.h"

class RelationalOp {
   public:
    // blocks the caller until the particular relational operator
    // has run to completion
    virtual void WaitUntilDone() = 0;

    // tell us how much internal memory the operation can use
    virtual void Use_n_Pages(int n) = 0;
};

class SelectFile : public RelationalOp {
    friend void *SelectFileProxyFunction(void *foo_ptr, void *args);

   private:
    pthread_t thread;
    Record *tempRec = new Record;

   public:
    void *Worker(void *args);
    void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};

class SelectPipe : public RelationalOp {
    friend void *SelectPipeProxyFunction(void *foo_ptr, void *args);

   private:
    pthread_t thread;
    Record *tempRec = new Record;

   public:
    void *Worker(void *args);
    void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};
class Project : public RelationalOp {
    friend void *ProjectProxyFunction(void *foo_ptr, void *args);

   private:
    pthread_t thread;
    Record *tempRec = new Record;

   public:
    void *Worker(void *args);
    void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {}
    void WaitUntilDone() {}
    void Use_n_Pages(int n) {}
};
class Join : public RelationalOp {
   public:
    void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {}
    void WaitUntilDone() {}
    void Use_n_Pages(int n) {}
};
class DuplicateRemoval : public RelationalOp {
   public:
    void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {}
    void WaitUntilDone() {}
    void Use_n_Pages(int n) {}
};
class Sum : public RelationalOp {
   public:
    void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {}
    void WaitUntilDone() {}
    void Use_n_Pages(int n) {}
};
class GroupBy : public RelationalOp {
   public:
    void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {}
    void WaitUntilDone() {}
    void Use_n_Pages(int n) {}
};
class WriteOut : public RelationalOp {
   public:
    void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {}
    void WaitUntilDone() {}
    void Use_n_Pages(int n) {}
};
#endif
