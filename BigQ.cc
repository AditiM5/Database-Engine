#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	tempRec = new (std::nothrow) Record();

	if (tempRec == NULL) {
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	currentPage = new (std::nothrow) Page();

	if (currentPage == NULL) {
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	file = new File();
	file->Open(0, "tempSortFile");

	// initialize current page number
	currPageNum = 0;

	while(in.Remove(tempRec)) {
		if (!currentPage->Append(tempRec)) {
            WriteCurrentPageToDisk();
            // empty the page out
            delete currentPage;
            currentPage = new(std::nothrow) Page();
            // append record to empty page
            currentPage->Append(tempRec);
        }
		
	}
	

	// read data from in pipe sort them into runlen pages



    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

void BigQ::WriteCurrentPageToDisk() {
    if (!currentPage->pageToDisk) {
        if (!file->GetLength()) {
            file->AddPage(currentPage, file->GetLength());
        } else {
            file->AddPage(currentPage, file->GetLength() - 1);
        }
//        fsync(file->myFilDes);
        currentPage->pageToDisk = true;
    }
}

BigQ::~BigQ () {
}
