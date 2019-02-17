#include "PriorityQueue.h"

using namespace std;

// return parent of A[i]
// don't call this function if i is already a root node
int PriorityQueue::PARENT(int i)
{
	return (i - 1) / 2;
}

// return left child of A[i]
int PriorityQueue::LEFT(int i)
{
	return (2 * i + 1);
}

// return right child of A[i]
int PriorityQueue::RIGHT(int i)
{
	return (2 * i + 2);
}

void PriorityQueue::swap(Record *rec1, Record *rec2)
{
	Record *temp = new Record();
	temp->Consume(rec1);
	rec1->Consume(rec2);
	rec2->Consume(temp);

	delete temp;
}

// Recursive Heapify-down algorithm
// the node at index i and its two direct children
// violates the heap property
void PriorityQueue::heapify_down(int i)
{
	// get left and right child of node at index i
	int left = LEFT(i);
	int right = RIGHT(i);

	int smallest = i;

	// compare A[i] with its left and right child
	// and find smallest value
	if (left < size && ceng.Compare(&A[left], &A[i], sortorder) < 0)
		smallest = left;

	if (right < size && ceng.Compare(&A[right], &A[smallest], sortorder) < 0)
		smallest = right;

	// swap with child having lesser value and
	// call heapify-down on the child
	if (smallest != i)
	{
		swap((A + i), (A + smallest));
		heapify_down(smallest);
	}
}

// Recursive Heapify-up algorithm
void PriorityQueue::heapify_up(int i)
{
	// check if node at index i and its parent violates
	// the heap property
	if (i && ceng.Compare(&A[PARENT(i)], &A[i], sortorder) > 0)
	{
		// swap the two if heap property is violated
		swap((A + i), (A + PARENT(i)));

		// call Heapify-up on the parent
		heapify_up(PARENT(i));
	}
}

PriorityQueue::PriorityQueue(OrderMaker *sort, int size)
{
	sortorder = sort;
	A = new Record[size];
}

// return size of the heap
int PriorityQueue::getLength()
{
	return size;
}

// function to check if heap is empty or not
bool PriorityQueue::empty()
{
	return size == 0;
}

// insert key into the heap
void PriorityQueue::push(Record *key)
{
	// insert the new element to the end of the vector
	(A + size++)->Consume(key);
	// A.push_back(key);

	// get element index and call heapify-up procedure
	int index = size - 1;
	heapify_up(index);
}

// function to remove element with lowest priority (present at root)
void PriorityQueue::pop()
{
	// if heap has no elements, throw an exception
	if (size == 0)
	{
		cout << "PQ empty" << endl;
		exit(1);
	}
	// replace the root of the heap with the last element
	// of the vector
	A->Consume((A + size--));
	// A[0] = A.back();
	// A.pop_back();

	// call heapify-down on root node
	heapify_down(0);
}

// function to return element with lowest priority (present at root)
void PriorityQueue::top(Record *tempRec)
{
	// if heap has no elements, throw an exception
	if (size == 0)
	{
		cout << "PQ empty" << endl;
		exit(1);
	}
	// else return the top (first) element
	tempRec->Copy(A);
	// return A.at(0);	// or return A[0];
}