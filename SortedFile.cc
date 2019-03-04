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
    readMode = true;
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

    Record temp;
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        input->Insert(&temp);
    }
    // input->ShutDown();
    // WritePipeToDisk(output);

    // // set it to read Mode
    // readMode = true;

    // delete input;
    // delete output;
    // delete bigq;
    // bigq = NULL;

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
    // change mode to writing
    readMode = false;
    initQ();
    input->Insert(rec);

    prevGetNext = false;
}

int SortedFile::GetNext(Record &fetchme) {
    WriteToRead();

    prevGetNext = false;
    return GetNextRecord(&fetchme);
}

// GetNext returns zero if not found
int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    Schema myschema("catalog", "lineitem");
    OrderMaker *newOrderMaker = new OrderMaker();
    if (!prevGetNext) {
        cnf.CreateQueryOrderMaker(sortOrder, newOrderMaker);
    }

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
                if (result != -1) {
                    break;
                }

                if (currPageNum + 1 < file->GetLength() - 1) {
                    currPageNum++;
                    file->GetPage(currentPage, currPageNum);
                } else {
                    break;
                }
            }

            if (result == -1) {
                return 0;
            }

            prevGetNext = true;

            // get the page where the record has been found
            file->GetPage(currentPage, currPageNum);
            currentPage->GetRecord(&fetchme, result - 1);
        }

        while (true) {
            // scan the current loaded page
            while (currentPage->GetFirst(&fetchme)) {
                if (ceng.CompareLit(&fetchme, &literal, newOrderMaker) == 0 && (ceng.Compare(&fetchme, &literal, &cnf) == 1)) {
                    return 1;
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
                    return 1;
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

void SortedFile::WriteToRead() {
    Schema mySchema("catalog", "lineitem");
    if (!readMode) {
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
                // left is greater than right
                if (ceng.Compare(fromFile, tempRec, sortOrder) >= 0)
                    break;
            }

            int tempCurrPageNum = currPageNum;
            Pipe *tempInput, *tempOutput;
            tempInput = new Pipe(100);
            tempOutput = new Pipe(100);
            BigQ *tempQ = new BigQ(*tempInput, *tempOutput, *sortOrder, 10);
            // move to the first record of the
            file->GetPage(currentPage, tempCurrPageNum);
            currPageNum = tempCurrPageNum;

            while (GetNextRecord(fromFile) == 1) {
                tempInput->Insert(fromFile);
            }

            // add the first removed record also into the pipe
            tempInput->Insert(tempRec);

            while (output->Remove(tempRec)) {
                tempInput->Insert(tempRec);
            }

            tempInput->ShutDown();

            // empty the page before adding everything back
            // currentPage->EmptyItOut();
            delete currentPage;
            currentPage = new Page();
            currPageNum = tempCurrPageNum;

            WritePipeToDiskOverWrite(tempOutput);

            tempOutput->ShutDown();
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
        tempRec->ClearRecord();
    }
}

void SortedFile::MoveFirst() {
    WriteToRead();
    // WriteCurrentPageToDisk();
    file->GetPage(currentPage, 0);
    currPageNum = 0;
    prevGetNext = false;
}

int SortedFile::Close() {
    WriteToRead();
    // WriteCurrentPageToDisk();
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

void SortedFile::WritePipeToDiskOverWrite(Pipe *output) {
    while (output->Remove(tempRec)) {
        if (!currentPage->Append(tempRec)) {
            PageToDiskOverWrite();
            currPageNum++;
            // empty the page out
            delete currentPage;
            currentPage = new (std::nothrow) Page();
            // append record to empty page
            currentPage->Append(tempRec);
        }
    }
    PageToDiskOverWrite();
}

// recursive BinarySearch lookup
// int SortedFile::BinarySearch(Record *recs, int start, int end, Record *key, OrderMaker *sortOrder) {
//     if (start <= end) {
//         int mid = start + (end - start) / 2;
//         ComparisonEngine ceng;

//         if (ceng.Compare((recs + mid), key, sortOrder) > 0) {
//             // if left is greater than right
//             // end = mid - 1;
//             return BinarySearch(recs, start, mid - 1, key, sortOrder);
//         } else if (ceng.Compare((recs + mid), key, sortOrder) < 0) {
//             // if left is less than right
//             // start = mid + 1;
//             return BinarySearch(recs, mid + 1, end, key, sortOrder);
//         } else if (start != mid) {
//             return BinarySearch(recs, start, mid, key, sortOrder);
//         } else {
//             return mid;
//         }
//     }
//     return -1;
// }

// Iterative BinarySearch lookup - Faster
int SortedFile::BinarySearch(Record *recs, int start, int end, Record *key, OrderMaker *sortOrder) {
    int index = -1;
    while (start <= end) {
        int mid = start + (end - start) / 2;

        ComparisonEngine ceng;

        if (ceng.CompareLit((recs + mid), key, sortOrder) == 0) {
            index = mid;
            end = mid - 1;
        } else if (ceng.CompareLit((recs + mid), key, sortOrder) > 0) {
            // if left is greater than right
            end = mid - 1;
        } else if (ceng.CompareLit((recs + mid), key, sortOrder) < 0) {
            // if left is less than right
            start = mid + 1;
        }
    }
    return index;
}