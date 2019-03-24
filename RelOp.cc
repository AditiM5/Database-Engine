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

    cout << "Temp rec addr: " << tempRec << endl;

    cout << "Num atts " << numAttsInput << endl;
    cout << "Is done? " << in->isDone() << endl;
    cout << "Rec empty?: " << tempRec->IsRecordEmpty() << endl;

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
    cout << "Entered Join Worker..." << endl;
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
        cout << "Stage 1 : Gotten the 2 sort orders..." << endl;
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
        // cout << "left file: " << tempString << endl;

        const char *temp_right_filename;
        int randomNumR = rand() % 128;
        milliseconds msR = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        string tempFileStringR = "temp_right" + to_string(msR.count()) + to_string(randomNumR) + ".bin";
        temp_right_filename = tempFileStringR.c_str();
        // cout << "right file: " << tempFileStringR << endl;

        DBFile left_buffer;
        left_buffer.Create(temp_left_filename, heap, NULL);

        // cout << "Weeeeee!!!!" << endl;

        DBFile right_buffer;
        right_buffer.Create(temp_right_filename, heap, NULL);

        // cout << "Stage 2 : Count Recs" << endl;

        // cout << "Order Makers" << endl;
        // left->Print();
        // right->Print();

        // count for the number of recs matched in the right pipe
        int count = 0;
        int left_check = outLeft->Remove(&tempRecLeft);
        int right_check = outRight->Remove(&tempRecRight);
        while (left_check != 0) {
            count = 0;
            // cause remove from pipe has failed when == 0
            while (right_check != 0) {
                // cout << "Stage 2 : Right recs loop" << endl;
                int result = ceng.Compare(&tempRecLeft, left, &tempRecRight, right);
                // cout << "result: " << result << endl;
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

            // cout << "Count: " << count << endl;
            // cout << "Stage 3: mergeing the buffer recs" << endl;
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
                // cout << "Number of atts for left : " << left_atts << endl;
                left_buffer.MoveFirst();

                // get the first rec
                right_buffer.GetNext(tempR);
                int right_atts = tempR.NumberOfAtts();
                // cout << "Number of atts for right : " << right_atts << endl;
                right_buffer.MoveFirst();

                int *total_atts = new int[left_atts + right_atts];

                // cout << "the atts array: " << endl;
                int k = 0;
                for (int i = 0; i < (left_atts + right_atts); i++) {
                    if (i == left_atts) {
                        // reset k so that array is 0..left_atts and 0..right_atts
                        // start of right therefore will be left_atts
                        k = 0;
                    }
                    *(total_atts + i) = k;
                    // cout << " i: " << i << " k: " << k << endl;
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
            // cout << "Last stage" << endl;
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
        //TODO: BNL!!!!!
    }
    outPipe->ShutDown();
    pthread_exit(NULL);
}

void Join::WaitUntilDone() {
    pthread_join(thread, NULL);
}