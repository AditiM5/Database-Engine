#include <iostream>
#include "DBFile.h"
#include "test.h"

using namespace std::chrono;

// make sure that the file path/dir information below is correct
const char *dbfile_dir = "temp/"; // dir where binary heap files should be stored
const char *tpch_dir = "data/"; // dir where dbgen tpch files (extension *.tbl) can be found
const char *catalog_path = "catalog"; // full path of the catalog file

using namespace std;

relation *rel;

// load from a tpch file
void test1() {

    DBFile dbfile;
    cout << " DBFile will be created at " << rel->path() << endl;
    dbfile.Create(rel->path(), heap, NULL);


    cout << "Created the file successfully!" << endl;

    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    cout << " tpch file will be loaded from " << tbl_path << endl;

    auto start = high_resolution_clock::now();
    dbfile.Load(*(rel->schema()), tbl_path);
    auto stop = high_resolution_clock::now();

    auto duration = duration_cast<microseconds>(stop - start);

    cout << "Time taken by function: "
         << duration.count() / 1000000 << "seconds" << endl;
    dbfile.Close();
}

// sequential scan of a DBfile 
void test2() {

    DBFile dbfile;
    dbfile.Open(rel->path());
    dbfile.MoveFirst();

    Record temp;

    int counter = 0;
    while (dbfile.GetNext(temp) == 1) {
        counter += 1;
        temp.Print(rel->schema());
        if (counter % 10000 == 0) {
            cout << counter << "\n";
        }
    }
    cout << " scanned " << counter << " recs \n";
    dbfile.Close();
}

// scan of a DBfile and apply a filter predicate
void test3() {
    cout << " Filter with CNF for : " << rel->name() << "\n";
    CNF cnf;
    Record literal;
    rel->get_cnf(cnf, literal);

    DBFile dbfile;
    dbfile.Open(rel->path());
    dbfile.MoveFirst();

    Record temp;

    int counter = 0;
    while (dbfile.GetNext(temp, cnf, literal) == 1) {
        counter += 1;
        temp.Print(rel->schema());
        if (counter % 10000 == 0) {
            cout << counter << "\n";
        }
    }
    cout << " selected " << counter << " recs \n";
    dbfile.Close();
}

// test adding of records to dbfile
void test4() {
    DBFile dbfile;
    cout << " DBFile will be created at " << rel->path() << endl;
    dbfile.Create(rel->path(), heap, NULL);
    cout << "Created the file successfully!" << endl;

    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    cout << " tpch file will be loaded from " << tbl_path << endl;

    FILE *tableFile = fopen(tbl_path, "r");

    int count = 0;
    Record tempRec;
    while (tempRec.SuckNextRecord(rel->schema(), tableFile) == 1) {
        dbfile.Add(&tempRec);
        count++;
    }
    dbfile.Close();
    cout << "Written " << count << " records" << endl;
}

// adding records and getNext
void test5() {
    DBFile dbfile;
    cout << " DBFile will be created at " << rel->path() << endl;
    dbfile.Create(rel->path(), heap, NULL);
    cout << "Created the file successfully!" << endl;

    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    cout << " tpch file will be loaded from " << tbl_path << endl;

    FILE *tableFile = fopen(tbl_path, "r");

    //added records once
    int count = 0;
    Record tempRec;
    while (tempRec.SuckNextRecord(rel->schema(), tableFile) == 1) {
        dbfile.Add(&tempRec);
        count++;
    }
    cout<<"Added "<<count<<" records "<<endl;

    //scanning
    dbfile.MoveFirst();
    count = 0;
    while (dbfile.GetNext(tempRec)) {
        count++;
    }
    cout << "Scanned " << count << " records" << endl;
    fseek(tableFile, 0, SEEK_SET);

    // add records again
    count = 0;
    while (tempRec.SuckNextRecord(rel->schema(), tableFile) == 1) {
        dbfile.Add(&tempRec);
        count++;
    }
    cout<<"Added "<<count<<" records again"<<endl;

    dbfile.MoveFirst();

    //scanning again
    dbfile.MoveFirst();
    count = 0;
    while (dbfile.GetNext(tempRec)) {
        count++;
    }
    cout << "Scanned " << count << " records" << endl;
    dbfile.Close();
}

int main() {

    setup(catalog_path, dbfile_dir, tpch_dir);

    void (*test)();
    relation *rel_ptr[] = {n, r, c, p, ps, o, li};
    void (*test_ptr[])() = {&test1, &test2, &test3, &test4, &test5};

    int tindx = 0;
    while (tindx < 1 || tindx > 5) {
        cout << " select test: \n";
        cout << " \t 1. load file \n";
        cout << " \t 2. scan \n";
        cout << " \t 3. scan & filter \n";
        cout << " \t 4. add to file \n ";
        cout << " \t 5. add and scan \n \t ";
        cin >> tindx;
    }

    int findx = 0;
    while (findx < 1 || findx > 7) {
        cout << "\n select table: \n";
        cout << "\t 1. nation \n";
        cout << "\t 2. region \n";
        cout << "\t 3. customer \n";
        cout << "\t 4. part \n";
        cout << "\t 5. partsupp \n";
        cout << "\t 6. orders \n";
        cout << "\t 7. lineitem \n \t ";
        cin >> findx;
    }

    rel = rel_ptr[findx - 1];
    test = test_ptr[tindx - 1];

    test();

    cleanup();
}
