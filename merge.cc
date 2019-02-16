
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include <pthread.h>

using namespace std;

void Merge(int records[], int start, int mid, int end) {
    // create a temp array
    int temp[end - start + 1];

    // crawlers for both intervals and for temp
    int i = start, j = mid + 1, k = 0;

    // traverse both arrays and in each iteration add smaller of both elements in temp
    while (i <= mid && j <= end) {
        if (records[i] < records[j]) {
            temp[k] = records[i];
            k++;
            i++;
        } else {
            temp[k] = records[j];
            k++;
            j++;
        }
    }

    // add elements left in the first interval
    while (i <= mid) {
        temp[k] = records[i];
        k++;
        i++;
    }

    // add elements left in the second interval
    while (j <= end) {
        temp[k] = records[j];
        k++;
        j++;
    }

    // copy temp to original interval
    for (i = start; i <= end; i += 1) {
        records[i] = temp[i - start];
    }
}

void MergeSort(int records[], int start, int end) {
    if (start < end) {
        int mid = (start + end) / 2;

        MergeSort(records, start, mid);
        MergeSort(records, mid + 1, end);
        Merge(records, start, mid, end);
    }
}

int main() {
    int test[] = {4, 9, 238, 77, 6, -5, 4, 323, 2 , 2341};
    MergeSort(test, 0, 9);
    for(int i = 0; i < 10; i++)
    {
        cout << test[i] << endl;
    }
    
    return 0;
}




