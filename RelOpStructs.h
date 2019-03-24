#ifndef REL_OP_S
#define REL_OP_S

#include "RelOp.h"

struct SelectPipeParams {
    Pipe *inPipe;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
    SelectPipe *ref;
};

struct SelectFileParams {
    DBFile *inFile;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
    SelectFile *ref;
};

struct ProjectParams {
    Pipe *inPipe;
    Pipe *outPipe;
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;
    Project *ref;
};

struct JoinParams {
    Pipe *inPipeL;
    Pipe *inPipeR;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
    Join *ref;
};

struct DuplicateRemovalParams {
    Pipe *inPipe;
    Pipe *outPipe;
    Schema *mySchema;
    DuplicateRemoval *ref;
};

struct SumParams{
    Pipe *inPipe;
    Pipe *outPipe;
    Function *computeMe;
    Sum *ref;
};

#endif