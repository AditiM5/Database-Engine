#include "RelOp.h"
#include <unistd.h>
#include <chrono>
#include "RelOpStructs.h"

using namespace std;
using namespace std::chrono;

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
    inFile->Close();
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
    ProjectParams *params;
    params = (ProjectParams *)args;
    void *fooPtr = params->ref;
    static_cast<Project *>(fooPtr)->Worker(args);
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

void *Project::Worker(void *args) {
    ProjectParams *params = (ProjectParams *)args;

    Pipe *in = params->inPipe;
    Pipe *out = params->outPipe;
    int *keepMe = params->keepMe;
    int numAttsInput = params->numAttsInput;
    int numAttsOutput = params->numAttsOutput;

    Record *tempRec = new Record;

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

void *JoinProxyFunction(void *args) {
    JoinParams *params;
    params = (JoinParams *)args;
    void *fooPtr = params->ref;
    static_cast<Join *>(fooPtr)->Worker(args);
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    JoinParams *params = new JoinParams;
    params->inPipeL = &inPipeL;
    params->inPipeR = &inPipeR;
    params->outPipe = &outPipe;
    params->selOp = &selOp;
    params->literal = &literal;
    params->ref = this;

    pthread_create(&thread, NULL, JoinProxyFunction, (void *)params);
}

void Join::Use_n_Pages(int n) {
    // get page size
    num_pages = n;
}

void *Join::Worker(void *args) {
    JoinParams *params = (JoinParams *)args;
    Pipe *inLeft = params->inPipeL;
    Pipe *inRight = params->inPipeR;
    Pipe *outPipe = params->outPipe;
    CNF *selOp = params->selOp;
    Record *literal = params->literal;

    ComparisonEngine ceng;
    OrderMaker *left = new OrderMaker;
    OrderMaker *right = new OrderMaker;

    if (selOp->GetSortOrders(*left, *right)) {
        // initialise 2 queues one for left and one for the right
        Pipe *outLeft = new Pipe(100);
        BigQ *q_left = new BigQ(*inLeft, *outLeft, *left, num_pages);

        Pipe *outRight = new Pipe(100);
        BigQ *q_right = new BigQ(*inRight, *outRight, *right, num_pages);

        Record tempRecLeft, tempRecRight;

        // naming the 2 temp DBFile used for Buffering
        const char *temp_left_filename;
        int randomNumL = rand() % 128;
        milliseconds msL = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        string tempFileStringL = "temp_left" + to_string(msL.count()) + to_string(randomNumL) + ".bin";
        temp_left_filename = tempFileStringL.c_str();
        string tempString(temp_left_filename);

        const char *temp_right_filename;
        int randomNumR = rand() % 128;
        milliseconds msR = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        string tempFileStringR = "temp_right" + to_string(msR.count()) + to_string(randomNumR) + ".bin";
        temp_right_filename = tempFileStringR.c_str();

        DBFile left_buffer;
        left_buffer.Create(temp_left_filename, heap, NULL);

        DBFile right_buffer;
        right_buffer.Create(temp_right_filename, heap, NULL);

        // count for the number of recs matched in the right pipe
        int count = 0;
        int left_check = outLeft->Remove(&tempRecLeft);
        int right_check = outRight->Remove(&tempRecRight);
        while (left_check != 0) {
            count = 0;
            // cause remove from pipe has failed when == 0
            while (right_check != 0) {
                int result = ceng.Compare(&tempRecLeft, left, &tempRecRight, right);
                if (result == 0) {
                    count++;
                    right_buffer.Add(&tempRecRight);
                    right_check = outRight->Remove(&tempRecRight);
                } else if (result == -1) {
                    break;
                } else {
                    right_check = outRight->Remove(&tempRecRight);
                }
            }
            // tempRecRight has the first rec of the next iteration

            if (count != 0) {
                // tempRec stores the first rec in left_buffer
                tempRec->Copy(&tempRecLeft);
                left_buffer.Add(&tempRecLeft);
                // to get all the current Y values from left pipe
                left_check = outLeft->Remove(&tempRecLeft);
                while (left_check != 0) {
                    if (ceng.Compare(tempRec, &tempRecLeft, left) == 0) {
                        left_buffer.Add(&tempRecLeft);
                        left_check = outLeft->Remove(&tempRecLeft);
                    } else {
                        break;
                    }
                    // on breaking tempRecLeft will have the first rec for the next iteration!!!
                }

                left_buffer.MoveFirst();
                right_buffer.MoveFirst();
                Record tempL, tempR;
                // get the first rec
                left_buffer.GetNext(tempL);
                int left_atts = tempL.NumberOfAtts();
                left_buffer.MoveFirst();

                // get the first rec
                right_buffer.GetNext(tempR);
                int right_atts = tempR.NumberOfAtts();
                right_buffer.MoveFirst();

                int *total_atts = new int[left_atts + right_atts];

                int k = 0;
                for (int i = 0; i < (left_atts + right_atts); i++) {
                    if (i == left_atts) {
                        // reset k so that array is 0..left_atts and 0..right_atts
                        // start of right therefore will be left_atts
                        k = 0;
                    }
                    *(total_atts + i) = k;
                    k++;
                }

                while (left_buffer.GetNext(tempL)) {
                    // for each left we want to iterate over all right
                    right_buffer.MoveFirst();
                    while (right_buffer.GetNext(tempR)) {
                        Record mergeRec;
                        mergeRec.MergeRecords(&tempL, &tempR, left_atts, right_atts, total_atts, (left_atts + right_atts),
                                              left_atts);
                        outPipe->Insert(&mergeRec);
                    }
                }

                // clear out the DBFiles
                left_buffer.Close();
                right_buffer.Close();

                left_buffer.Create(temp_left_filename, heap, NULL);
                right_buffer.Create(temp_right_filename, heap, NULL);
            } else {
                // Get next left record for the next outer loop iteration
                left_check = outLeft->Remove(&tempRecLeft);
            }
        }

        // clean up of tempfiles
        unlink(temp_left_filename);
        unlink(temp_right_filename);
        // cleanup .data files
        tempFileStringL += ".data";
        unlink(tempFileStringL.c_str());
        tempFileStringR += ".data";
        unlink(tempFileStringR.c_str());
    } else {
        // BNL
        DBFile left_buffer;
        DBFile right_buffer;

        const char *temp_left_filename;
        int randomNumL = rand() % 128;
        milliseconds msL = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        string tempFileStringL = "temp_left" + to_string(msL.count()) + to_string(randomNumL) + ".bin";
        temp_left_filename = tempFileStringL.c_str();
        string tempString(temp_left_filename);

        const char *temp_right_filename;
        int randomNumR = rand() % 128;
        milliseconds msR = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        string tempFileStringR = "temp_right" + to_string(msR.count()) + to_string(randomNumR) + ".bin";
        temp_right_filename = tempFileStringR.c_str();

        left_buffer.Create(temp_left_filename, heap, NULL);
        right_buffer.Create(temp_right_filename, heap, NULL);

        WritePipeToFile(&left_buffer, inLeft);
        WritePipeToFile(&right_buffer, inRight);

        left_buffer.MoveFirst();
        right_buffer.MoveFirst();
        Record tempRec_L, tempRec_R;

        left_buffer.GetNext(tempRec_L);
        int left_atts = tempRec_L.NumberOfAtts();
        left_buffer.MoveFirst();

        // get the first rec
        right_buffer.GetNext(tempRec_R);
        int right_atts = tempRec_R.NumberOfAtts();
        right_buffer.MoveFirst();

        int *total_atts = new int[left_atts + right_atts];

        int k = 0;
        for (int i = 0; i < (left_atts + right_atts); i++) {
            if (i == left_atts) {
                // reset k so that array is 0..left_atts and 0..right_atts
                // start of right therefore will be left_atts
                k = 0;
            }
            *(total_atts + i) = k;
            k++;
        }
        ComparisonEngine ceng;

        while (left_buffer.GetNext(tempRec_L)) {
            right_buffer.MoveFirst();
            while (right_buffer.GetNext(tempRec_R)) {
                if (ceng.Compare(&tempRec_L, &tempRec_R, literal, selOp)) {
                    Record mergeRec;
                    mergeRec.MergeRecords(&tempRec_L, &tempRec_R, left_atts, right_atts, total_atts, (left_atts + right_atts),
                                          left_atts);
                    outPipe->Insert(&mergeRec);
                }
            }
        }
        // clean up of tempfiles
        unlink(temp_left_filename);
        unlink(temp_right_filename);
        // cleanup .data files
        tempFileStringL += ".data";
        unlink(tempFileStringL.c_str());
        tempFileStringR += ".data";
        unlink(tempFileStringR.c_str());
    }
    outPipe->ShutDown();
    pthread_exit(NULL);
}

void Join::WaitUntilDone() {
    pthread_join(thread, NULL);
}

const char *RelationalOp::GenTempFileName() {
    const char *temp_filename;
    int randomNum = rand() % 128;
    milliseconds ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    string tempFileString = "temp" + to_string(ms.count()) + to_string(randomNum) + ".bin";
    temp_filename = tempFileString.c_str();
    return temp_filename;
}

void RelationalOp::WritePipeToFile(DBFile *dbfile, Pipe *pipe) {
    Record tempRec;
    while (pipe->Remove(&tempRec)) {
        dbfile->Add(&tempRec);
    }
}

void *DuplicateRemoval::Worker(void *args) {
    DuplicateRemovalParams *params = (DuplicateRemovalParams *)args;
    Pipe *inPipe;
    Pipe *outPipe;
    Schema *mySchema;

    inPipe = params->inPipe;
    outPipe = params->outPipe;
    mySchema = params->mySchema;

    OrderMaker sortOrder(mySchema);

    Pipe *bigq_out = new Pipe(100);
    BigQ *bigq = new BigQ(*inPipe, *bigq_out, sortOrder, num_pages);
    Record curr_rec, tempRec;

    bigq_out->Remove(&curr_rec);
    tempRec.Copy(&curr_rec);
    outPipe->Insert(&tempRec);

    ComparisonEngine ceng;
    while (bigq_out->Remove(&tempRec)) {
        int result = ceng.Compare(&curr_rec, &tempRec, &sortOrder);
        if (result == -1) {
            curr_rec.Copy(&tempRec);
            outPipe->Insert(&tempRec);
        }
    }

    outPipe->ShutDown();
}

void *DuplicateRemovalProxyFunction(void *args) {
    DuplicateRemovalParams *params;
    params = (DuplicateRemovalParams *)args;
    void *fooPtr = params->ref;
    static_cast<DuplicateRemoval *>(fooPtr)->Worker(args);
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    DuplicateRemovalParams *params = new DuplicateRemovalParams;
    params->inPipe = &inPipe;
    params->outPipe = &outPipe;
    params->mySchema = &mySchema;
    params->ref = this;

    pthread_create(&thread, NULL, DuplicateRemovalProxyFunction, (void *)params);
}

void DuplicateRemoval::Use_n_Pages(int n) {
    this->num_pages = n;
}

void DuplicateRemoval::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void *SumProxyFunction(void *args) {
    SumParams *params;
    params = (SumParams *)args;
    void *fooPtr = params->ref;
    static_cast<Sum *>(fooPtr)->Worker(args);
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    SumParams *params = new SumParams;
    params->inPipe = &inPipe;
    params->outPipe = &outPipe;
    params->computeMe = &computeMe;
    params->ref = this;

    pthread_create(&thread, NULL, SumProxyFunction, (void *)params);
}

void *Sum::Worker(void *args) {
    SumParams *params = (SumParams *)args;

    Pipe *inPipe = params->inPipe;
    Pipe *outPipe = params->outPipe;
    Function *computeMe = params->computeMe;

    int result_i = 0;
    double result_d = 0;
    int temp_result_i = 0;
    double temp_result_d = 0;
    Type result;

    while (inPipe->Remove(tempRec)) {
        result = computeMe->Apply(*tempRec, temp_result_i, temp_result_d);
        if (result == Int) {
            result_i += temp_result_i;
        } else {
            result_d += temp_result_d;
        }
    }

    char *space = new (std::nothrow) char[PAGE_SIZE];
    int totspace = sizeof(int) * 2;

    if (result == Int) {
        totspace += sizeof(int);
        ((int *)space)[0] = totspace;
    } else {
        totspace += sizeof(double);
        ((int *)space)[0] = totspace;
    }

    ((int *)space)[1] = 8;

    if (result == Int) {
        *((int *)&(space[8])) = result_i;
    } else {
        *((double *)&(space[8])) = result_d;
    }

    // our new record that has the sum
    Record tempSum;
    tempSum.bits = new char[totspace];
    memcpy(tempSum.bits, space, totspace);
    delete[] space;

    outPipe->Insert(&tempSum);

    outPipe->ShutDown();
}

void Sum::Use_n_Pages(int n) {
}

void Sum::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void *GroupByProxyFunction(void *args) {
    GroupByParams *params;
    params = (GroupByParams *)args;
    void *fooPtr = params->ref;
    static_cast<GroupBy *>(fooPtr)->Worker(args);
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    GroupByParams *params = new GroupByParams;
    params->inPipe = &inPipe;
    params->outPipe = &outPipe;
    params->computeMe = &computeMe;
    params->groupAtts = &groupAtts;
    params->ref = this;

    pthread_create(&thread, NULL, GroupByProxyFunction, (void *)params);
}

void *GroupBy::Worker(void *args) {
    GroupByParams *params = (GroupByParams *)args;

    Pipe *inPipe = params->inPipe;
    Pipe *outPipe = params->outPipe;
    OrderMaker *groupAtts = params->groupAtts;
    Function *computeMe = params->computeMe;

    Pipe bigq_out(100);

    BigQ bigq(*inPipe, bigq_out, *groupAtts, num_pages);

    Record curr_rec;
    int result_i = 0, temp_result_i = 0;
    double result_d = 0, temp_result_d = 0;

    bigq_out.Remove(&curr_rec);
    Type result_type;

    result_type = computeMe->Apply(curr_rec, result_i, result_d);

    ComparisonEngine ceng;
    while (bigq_out.Remove(tempRec)) {
        int result = ceng.Compare(&curr_rec, tempRec, groupAtts);

        if (result == 0) {
            result_type = computeMe->Apply(*tempRec, temp_result_i, temp_result_d);
            result_i += temp_result_i;
            result_d += temp_result_d;
        } else if (result == -1) {
            Record sum;
            BuildRecord(&sum, &curr_rec, result_type, result_i, result_d, groupAtts);
            outPipe->Insert(&sum);

            curr_rec.Consume(tempRec);
            result_i = 0;
            result_d = 0;
            result_type = computeMe->Apply(curr_rec, result_i, result_d);
        }
    }

    Record sum;
    BuildRecord(&sum, &curr_rec, result_type, result_i, result_d, groupAtts);
    outPipe->Insert(&sum);
    outPipe->ShutDown();
}

void GroupBy::Use_n_Pages(int n) {
    this->num_pages = n;
}

void GroupBy::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void GroupBy::BuildRecord(Record *sum, Record *record, Type result, int result_i, double result_d, OrderMaker *groupAtts) {
    char *space = new (std::nothrow) char[PAGE_SIZE];
    int totspace = sizeof(int) * 2;

    ((int *)space)[1] = 8;

    if (result == Int) {
        totspace += sizeof(int);
        *((int *)&(space[8])) = result_i;
    } else {
        totspace += sizeof(double);
        *((double *)&(space[8])) = result_d;
    }

    ((int *)space)[0] = totspace;

    sum->bits = new char[totspace];
    memcpy(sum->bits, space, totspace);
    delete[] space;

    record->Project(groupAtts->whichAtts, groupAtts->numAtts, record->NumberOfAtts());

    int left_atts = 1;
    int right_atts = groupAtts->numAtts;
    int *total_atts = new int[left_atts + right_atts];

    int k = 0;
    for (int i = 0; i < (left_atts + right_atts); i++) {
        if (i == left_atts) {
            // reset k so that array is 0..left_atts and 0..right_atts
            // start of right therefore will be left_atts
            k = 0;
        }
        *(total_atts + i) = k;
        k++;
    }

    Attribute IA = {"int", Int};

    Record mergeRec;
    mergeRec.MergeRecords(sum, record, left_atts, right_atts, total_atts, (left_atts + right_atts), left_atts);

    int outAtts = 2;
    Attribute joinatt[] = {IA, IA};
    Schema join_sch("join_sch", outAtts, joinatt);

    sum->Consume(&mergeRec);
}

void WriteOut::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void *WriteOutProxyFunction(void *args) {
    WriteOutParams *params;
    params = (WriteOutParams *)args;
    void *fooPtr = params->ref;
    static_cast<WriteOut *>(fooPtr)->Worker(args);
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    WriteOutParams *params = new WriteOutParams;
    params->inPipe = &inPipe;
    params->outFile = outFile;
    params->mySchema = &mySchema;
    params->ref = this;

    pthread_create(&thread, NULL, WriteOutProxyFunction, (void *)params);
}

void* WriteOut::Worker(void *args) {
    WriteOutParams *params = (WriteOutParams *)args;

    Pipe *inPipe = params->inPipe;
    FILE *outFile = params->outFile;
    Schema *mySchema = params->mySchema;
    string s;

    while(inPipe->Remove(tempRec)) {
        s = tempRec->ToString(mySchema);
        fprintf(outFile, "%s\n", s.c_str());
    }
}