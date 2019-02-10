#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ {

	Record *tempRec;
	File *file;
	Page *currentPage;
	int *currPageNum;

	void WriteCurrentPageToDisk();
	void SortRecords(Page *page, OrderMaker *sortorder);
	void MergeSort(Record records[], int start, int end, OrderMaker *sortorder);
	void Merge(Record records[], int start, int mid, int end, OrderMaker *sortorder);

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
