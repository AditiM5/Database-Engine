#include "SortedFile.h"
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include "GenericDBFile.h"

using namespace std;

SortedFile::SortedFile() {
    currentPage = new (std::nothrow) Page();
    tempRec = new Record();
}

// create  a metadata file
int SortedFile::Create(const char *f_path, void *startup) {
    sortdata = (SortInfo *)startup;
    string metadataFileName(f_path);
    metadataFileName += ".data";
    FILE *metadata = fopen(metadataFileName.c_str(), "w");
    fprintf(metadata, "%s\n", "sorted");
    fprintf(metadata, "%d\n", sortdata->runLength);
    WriteOrderMaker(sortdata->myOrder, metadata);
    fclose(metadata);

    sortOrder = sortdata->myOrder;
    runLength = sortdata->runLength;

    file = new File();
    file->Open(0, f_path);

    prevGetNext = false;
    return 1;
}

void SortedFile::Load(Schema &f_schema, const char *loadpath) {
    initQ();
    readMode = false;
    FILE *tableFile = fopen(loadpath, "r");
    if (!tableFile) {
        cout << "ERROR : Cannot open file. EXIT !!!\n";
        exit(1);
    }

    int count = 0;
    Record temp;
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        input->Insert(&temp);
        count++;
    }
    input->ShutDown();
    WritePipeToDisk(output);

    // set it to read Mode
    readMode = true;

    delete input;
    delete output;
    delete bigq;
    bigq = NULL;

    prevGetNext = false;
}

int SortedFile::Open(const char *f_path) {
    readMode = true;
    string metadataFileName(f_path);
    metadataFileName += ".data";
    FILE *metadata = fopen(metadataFileName.c_str(), "r");

    char arr[10];
    fscanf(metadata, "%s", arr);
    fscanf(metadata, "%s", arr);

    runLength = atoi(arr);
    sortOrder = new OrderMaker;

    ReadOrderMaker(sortOrder, metadata);

    file = new File();
    file->Open(1, f_path);

    prevGetNext = false;

    return 1;
}

void SortedFile::initQ() {
    if (bigq == NULL) {
        input = new Pipe(100);
        output = new Pipe(100);
        bigq = new BigQ(*input, *output, *sortOrder, runLength);
    }
}

void SortedFile::Add(Record *rec) {
    initQ();
    input->Insert(rec);

    // change mode to writing
    readMode = false;
    prevGetNext = false;
}

int SortedFile::GetNext(Record &fetchme) {
    SwitchFromReadToWrite();
    prevGetNext = false;
    return GetNextRecord(&fetchme);
}

// GetNext returns zero if not found
int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    OrderMaker *newOrderMaker = new OrderMaker();
    if (!prevGetNext) {
        cnf.CreateQueryOrderMaker(sortOrder, newOrderMaker);
    }
    cout << "Printing query ordermaker!!!";
    newOrderMaker->Print();

    ComparisonEngine ceng;

    int result = -1;
    if (newOrderMaker->numAtts != 0) {
        if (!prevGetNext) {
            while (true) {
                int numRecords = currentPage->numRecs;
                Record *recs = new Record[numRecords];
                for (int i = 0; i < numRecords; i++) {
                    currentPage->GetFirst(recs + i);
                }
                result = BinarySearch(recs, 0, numRecords - 1, &literal, newOrderMaker);
                cout << "The result of bin search: " << result << endl;
                if (result != -1) {
                    cout << "Found and breaking from loop...." << endl;
                    break;
                }

                if (currPageNum + 1 < file->GetLength() - 1) {
                    currPageNum++;
                    file->GetPage(currentPage, currPageNum);
                } else {
                    break;
                }
            }

            cout << "Out of the first while true loop" << endl;
            if (result == -1) {
                return 0;
                cout << "Record not found!!!!!" << endl;
            }
            prevGetNext = true;

            // get the page where the record has been found
            file->GetPage(currentPage, currPageNum);
            currentPage->GetRecord(&fetchme, result - 1);
        }

        while (true) {
            while (currentPage->GetFirst(&fetchme)) {
                if (ceng.Compare(&fetchme, &literal, sortOrder) == 0 && (ceng.Compare(&fetchme, &literal, &cnf) == 1)) {
                    return 0;
                }
            }
            if (currPageNum + 1 < file->GetLength() - 1) {
                currPageNum++;
                file->GetPage(currentPage, currPageNum);
            } else {
                break;
            }
        }
    } else {
        while (true) {
            while (currentPage->GetFirst(&fetchme)) {
                if (ceng.Compare(&fetchme, &literal, &cnf) == 1) {
                    return 0;
                }
            }
            if (currPageNum + 1 < file->GetLength() - 1) {
                currPageNum++;
                file->GetPage(currentPage, currPageNum);
            } else {
                break;
            }
        }
    }
    return 0;
}

void SortedFile::SwitchFromReadToWrite() {
    if (!readMode) {
        // cout << "Stage 1: Entered!" << endl;

        input->ShutDown();
        if (file->GetLength() == 0) {
            WritePipeToDisk(output);
        } else {
            output->Remove(tempRec);
            file->GetPage(currentPage, 0);

            currPageNum = 0;
            ComparisonEngine ceng;
            Record *fromFile = new Record;
            while (GetNextRecord(fromFile) == 1) {
                if (ceng.Compare(tempRec, fromFile, sortOrder) > 0)
                    break;
            }

            // cout << "Stage 2:" << endl;

            int tempCurrPageNum = currPageNum;
            Pipe *tempInput, *tempOutput;
            tempInput = new Pipe(100);
            tempOutput = new Pipe(100);
            BigQ *tempQ = new BigQ(*tempInput, *tempOutput, *sortOrder, 10);
            // move to the first record of the
            file->GetPage(currentPage, tempCurrPageNum);
            currentPage->EmptyItOut();
            currPageNum = tempCurrPageNum;

            // cout << "Stage 3:" << endl;

            while (GetNextRecord(fromFile) == 1) {
                tempInput->Insert(fromFile);
            }

            // add the first removed record also into the pipe
            tempInput->Insert(tempRec);

            // cout << "Stage 4:" << endl;

            while (output->Remove(tempRec)) {
                tempInput->Insert(tempRec);
            }

            tempInput->ShutDown();

            // cout << "Stage 5:" << endl;
            WritePipeToDisk(tempOutput);
            tempOutput->ShutDown();
            // cout << "Stage 6:" << endl;
            // now switch to read mode / switch out of write mode
            delete tempInput;
            delete tempOutput;
            delete tempQ;
        }
        readMode = true;
        delete input;
        delete output;
        delete bigq;
        bigq = NULL;
    }
}

void SortedFile::MoveFirst() {
    cout << "Move First in SortedFile" << endl;
    SwitchFromReadToWrite();
    WriteCurrentPageToDisk();
    cout << "File len: " << file->GetLength() << endl;
    file->GetPage(currentPage, 0);
    currPageNum = 0;
    prevGetNext = false;
}

int SortedFile::Close() {
    cout << "\n Close First in SortedFile";
    SwitchFromReadToWrite();
    WriteCurrentPageToDisk();
    prevGetNext = false;

    fsync(file->myFilDes);
    // returns 1 on success
    if (!file->Close()) {
        return 1;
    } else {
        return 0;
    }
}

void SortedFile::WritePipeToDisk(Pipe *output) {
    while (output->Remove(tempRec)) {
        if (!currentPage->Append(tempRec)) {
            WriteCurrentPageToDisk();
            currPageNum++;
            // empty the page out
            delete currentPage;
            currentPage = new (std::nothrow) Page();
            // append record to empty page
            currentPage->Append(tempRec);
        }
    }
    WriteCurrentPageToDisk();
}

int SortedFile::BinarySearch(Record *recs, int start, int end, Record *key, OrderMaker *sortOrder) {
    if (start <= end) {
        int mid = start + (end - start) / 2;
        ComparisonEngine ceng;
        if (ceng.Compare((recs + mid), key, sortOrder) == 0) {
            return mid;
        } else if (ceng.Compare((recs + mid), key, sortOrder) > 0) {
            // if left is greater than right
            // end = mid - 1;
            return BinarySearch(recs, start, mid - 1, key, sortOrder);
        } else if (ceng.Compare((recs + mid), key, sortOrder) < 0) {
            // if left is less than right
            // start = mid + 1;
            return BinarySearch(recs, mid + 1, end, key, sortOrder);
        }
    }
    return -1;
}

// compare with Order Maker
// then compare with CNF
// return the AND of the two
bool SortedFile::CompareOrderMakerCNF(Record *record, Record *literal, OrderMaker *sortOrder, CNF *cnf) {
    ComparisonEngine ceng;

    if (ceng.Compare(record, literal, sortOrder) != 0) {
        return false;
    }

    if (ceng.Compare(record, literal, cnf) == 1) {
        return true;
    }

    return false;
}