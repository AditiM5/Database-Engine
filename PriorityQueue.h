#ifndef _PRIORITY_QUEUE
#define _PRIORITY_QUEUE

#include <iostream>
#include <unistd.h>
#include "Record.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

using namespace std;

// Data structure for Min Heap
class PriorityQueue 
{
private:
	// vector to store heap elements
	Record *A;
    OrderMaker *sortorder;
    ComparisonEngine ceng;
	int size = 0;

	// return parent of A[i]
	// don't call this function if i is already a root node
	int PARENT(int i);

	// return left child of A[i]	
	int LEFT(int i);

	// return right child of A[i]	
	int RIGHT(int i);

	void swap(Record *rec1, Record *rec2);

	// Recursive Heapify-down algorithm
	// the node at index i and its two direct children
	// violates the heap property
	void heapify_down(int i);

	// Recursive Heapify-up algorithm
	void heapify_up(int i);
	
public:
    PriorityQueue(OrderMaker *sort, int size);

	// return size of the heap
	int getLength();

	// function to check if heap is empty or not
	bool empty();
	
	// insert key into the heap
	void push(Record *key);

	// function to remove element with lowest priority (present at root)
	void pop();

	// function to return element with lowest priority (present at root)
	void top(Record *tempRec);
};

#endif