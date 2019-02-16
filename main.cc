
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include <pthread.h>
#include <unistd.h>

using namespace std;

void *producer(void *arg) {
    Pipe *pipe = (Pipe *) arg;
    FILE *tableFile = fopen("data/test.tbl", "r");
    Record temp;
    Schema myschema("catalog", "lineitem");
    int i = 0;

    while (temp.SuckNextRecord(&myschema, tableFile) == 1) {
        pipe->Insert(&temp);
        cout << "Inserting record: " << i++ << endl;
    }

    pipe->ShutDown();
    pthread_exit(NULL);
}

void *consumer(void *arg) {
    Pipe *pipe = (Pipe *) arg;
    Record temp;
    Schema myschema("catalog", "lineitem");
    int count = 0;

    while (pipe->Remove(&temp)) {
        temp.Print(&myschema);
        count++;
    }

    cout << "Removed count: " << count << endl;
    pthread_exit(NULL);
}

int main() {
    Pipe input(100);
    Pipe output(100);

    Schema myschema("catalog", "lineitem");
    Record temp;

    OrderMaker sortorder(&myschema);

    pthread_t thread1;
    pthread_t thread2;

    pthread_create(&thread1, NULL, consumer, (void *) &output);
    pthread_create(&thread1, NULL, producer, (void *) &input);

    BigQ bq(input, output, sortorder, 1);

    int i = 0;

    // usleep(2000000);

    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);
    pthread_join(bq.threadID, NULL);

    // cout << "ThreadID : " << bq.threadID << endl;

    return 0;
}




