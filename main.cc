
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <unordered_set>
#include "QueryPlan.h"

#define tables_dir "data-myTable/"

using namespace std;

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char *);
extern "C" void yy_delete_buffer(struct YY_BUFFER_STATE *);
extern "C" {
int yyparse(void);  // defined in y.tab.c
}

// these data structures hold the result of the parsing
extern struct FuncOperator *finalFunction;  // the aggregate function (NULL if no agg)
extern struct TableList *tables;            // the list of tables and aliases in the query
extern struct AndList *boolean;             // the predicate in the WHERE clause
extern struct NameList *groupingAtts;       // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect;       // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts;                    // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;                    // 1 if there is a DISTINCT in an aggregate query

extern struct SchemaList *schemas;               // the list of tables and aliases in the query
extern struct CreateTableType *createTableType;  // type of table to create along with sorting attributes (if any)
extern char *bulkFileName;                       // bulk loading file name string
extern char *outputFileName;                     // output file name or STDOUT string
extern int commandFlag;                          // 1 if the command is a create table command.

extern int NumAtt;

// set to store all the relations been added so far
unordered_set<string> rels;
// name of file to store all the relations names
char *rel_file = "relations.txt";
// name of the catalog file
char *cat_file = "catalog";
// std out
std::streambuf *coutbuf;
bool flag_exec = true;
bool op_stdout = true;

std::streambuf *stream_buffer_cout = NULL;
fstream file;

int main1() {
    std::ofstream out("query_plan_op/q13.txt");
    std::streambuf *coutbuf = std::cout.rdbuf();  //save old buf
    std::cout.rdbuf(out.rdbuf());

    char *query = "CREATE TABLE mytable (att1 INTEGER, att2 DOUBLE, att3 STRING) AS HEAP";
    // yy_scan_string(query);
    yyparse();
    char *statFileName = "Statistics.txt";
    Statistics *stats = new Statistics;
    stats->Read(statFileName);
    Query q(stats);
    q.QueryPlan();

    std::cout.rdbuf(coutbuf);

    return 0;
}

// CREATE
Schema *createTable(DBFile *dbfile) {
    string tableName = string(tables->tableName);

    // check if the rel exists
    int count = rels.count(tableName);
    cout << "\n Outside: count " << count;
    if (rels.count(tableName) != 0) {
        cout << "\n Inside: count " << count;
        return NULL;
    }

    cout << "\n TableName: " << tableName;

    // Attribute* myAtts = new Attribute[NumAtt];
    Attribute myAtts[NumAtt];

    int i = 0;
    while (schemas) {
        myAtts[i].name = strdup(schemas->attName);

        if (strcmp(schemas->type, "INTEGER") == 0)
            myAtts[i].myType = Int;
        else if (strcmp(schemas->type, "DOUBLE") == 0)
            myAtts[i].myType = Double;
        else if (strcmp(schemas->type, "STRING") == 0)
            myAtts[i].myType = String;

        schemas = schemas->next;
        i++;
    }

    Schema *schema = new Schema((char *)tableName.c_str(), i, myAtts);
    NumAtt = 0;
    string dir = string(tables_dir);
    string table_path = dir + tableName + ".bin";

    if (strcmp(createTableType->heapOrSorted, "HEAP") == 0) {
        dbfile->Create(table_path.c_str(), heap, NULL);
        cout << "\n Created a DBFile";
    }

    rels.insert(tableName);

    dbfile->Close();

    return schema;
}

void loadRelations() {
    ifstream infile;
    infile.open(rel_file);

    string buffer;
    while (getline(infile, buffer)) {
        rels.insert(buffer);
    }

    cout << "\n Inserted everything into the rels set";
    infile.close();
}

void writeSchemaToDisk(Schema *schema) {
    Attribute *atts = schema->GetAtts();
    string rel_name = schema->getfileName();

    // just write the relation names
    cout << "\n Relation Name in file : " << rel_name;
    ofstream infile;
    infile.open(rel_file, fstream::app);
    infile << rel_name << "\n";
    infile.close();

    ofstream catalog;
    catalog.open(cat_file, fstream::app);
    // write the initial crap BEGIN
    catalog << "\nBEGIN";
    catalog << "\n"
            << rel_name;
    catalog << "\n"
            << rel_name << ".tbl";

    for (int i = 0; i < schema->GetNumAtts(); i++) {
        string att_type;
        switch (atts[i].myType) {
            case Int:
                att_type = "Int";
                break;
            case Double:
                att_type = "Double";
                break;
            case String:
                att_type = "String";
                break;
        }
        catalog << "\n"
                << string(atts[i].name) << " " << att_type;
    }

    catalog << "\nEND\n";
    catalog.close();
}

void insert(DBFile *dbfile) {
    // check if the relname exists
    string table_name = string(tables->tableName);
    if (rels.count(table_name) == 0) {
        // rel_name does not exist
        cout << "\n ERROR! Relation does not exist";
        return;
    }

    Schema schema(cat_file, table_name.c_str());
    string temp = tables_dir + table_name + ".bin";
    dbfile->Open(temp.c_str());
    temp.clear();
    // temp = tables_dir + string(bulkFileName);

    // load the dbfile
    dbfile->Load(schema, bulkFileName);
    // dbfile->Load(schema, temp.c_str());
    dbfile->Close();

    cout << "\n Hey I loaded the file!";
}

void printQueryOutput(Query *q) {
    cout << "\n Printing output of the query";
    Record tempRec;
    int count = 0;
    while (q->root->outputPipe->Remove(&tempRec)) {
        tempRec.Print(q->root->outschema);
        count++;
        if (count % 1000 == 0) cout << "\n"
                                    << count++;
    }

    cout << "\n " << count;
}

void printQueryOutputToFile(Query *q) {
    cout << "\n Printing output of the query";

    FILE *file = fopen(outputFileName, "w");

    WriteOut wo;
    wo.Run(*(q->root->outputPipe), file, *(q->root->outschema));
    wo.WaitUntilDone();

    fclose(file);
    // cout << "\n " << count;
}

void dropTable() {
    string relName = string(tables->tableName);  // outputFileName->name
    if (rels.count(relName) == 0) {
        cout << "The table doesn't exist. Cannot drop." << endl;
        return;
    }
    string file_path = tables_dir + relName + ".bin";
    string meta_path = file_path + ".data";
    if (unlink(file_path.c_str()) != 0)
        perror("ERROR: Cannot delete .bin");
    if (unlink(meta_path.c_str()) != 0)
        perror("ERROR: Cannot delete .bin.meta");
    rels.erase(relName);

    cout << "\n Table Dropped";

    ifstream infile;
    infile.open(rel_file);
    string buffer;

    ofstream tempFile("temp");

    while (getline(infile, buffer)) {  // while the file has more lines.
        cout << "\n Buffer: " << buffer;
        if (buffer.compare(relName) != 0)
            tempFile << buffer << endl;
    }
    infile.close();
    tempFile.close();

    unlink(rel_file);
    rename("temp", rel_file);
}

void setOutput() {
    // std::cout.flush();
    // std::ofstream out(outputFileName);
    // coutbuf = std::cout.rdbuf();  //save old buf
    // std::cout.rdbuf(out.rdbuf());

    file.open(outputFileName, ios::out);

    stream_buffer_cout = std::cout.rdbuf();
    streambuf *stream_buffer_file = file.rdbuf();
    std::cout.rdbuf(stream_buffer_file);
}

void resetOutput() {
    // std::cout.rdbuf(coutbuf);
    if (stream_buffer_cout != NULL) {
        std::cout.flush();
        std::cout.rdbuf(stream_buffer_cout);
        file.close();
        stream_buffer_cout = NULL;
    }
}

void chooseOutput() {
    // string temp = "out.txt";
    // outputFileName = (char *) temp.c_str();
    if (strcmp(outputFileName, "STDOUT") == 0) {
        // reset
        // resetOutput();
        flag_exec = true;
        op_stdout = true;
    } else if (strcmp(outputFileName, "NONE") == 0) {
        flag_exec = false;
    } else {
        cout << "\n To reset to STDOUT type \'SET OUTPUT STDOUT\' \n";
        flag_exec = true;
        op_stdout = false;
        // setOutput();
    }
}

int main() {
    // set it to stdout
    // std::ofstream temp("qkwuhegfvhaegr.txt");
    // coutbuf = std::cout.rdbuf();  //save old buf
    // std::cout.rdbuf(temp.rdbuf());

    // std::cout.rdbuf(coutbuf);

    // run infinite loop to check for things
    DBFile *dbfile = NULL;
    Schema *schema = NULL;

    char *statFileName = "Statistics.txt";
    Statistics *stats = NULL;
    Query *q = NULL;

    cout << "\n Query Constructor.......";

    loadRelations();

    cout << "\n Stage 1";

    while (1) {
        string cmd;
        cout << "\n Enter Query: ";
        std::getline(cin, cmd);
        YY_BUFFER_STATE *buf = yy_scan_string(cmd.c_str());
        yyparse();
        cout << "\n Command Flag: " << commandFlag;

        switch (commandFlag) {
            case 1:
                dbfile = new DBFile;
                cout << "\n Create the TABLE.";
                schema = createTable(dbfile);
                if (schema == NULL) {
                    cout << "\n Relation already exists!";
                    break;
                } else {
                    // new relation thats why write to the disk
                    writeSchemaToDisk(schema);
                    cout << "\n Printing output schema......";
                    printOutputSchema(schema);
                }
                delete dbfile;
                dbfile = NULL;
                break;

            case 2:
                dbfile = new DBFile;
                insert(dbfile);
                delete dbfile;
                dbfile = NULL;
                break;

            case 3:
                dropTable();
                break;

            case 4:
                chooseOutput();
                break;

            case 5:
                stats = new Statistics;
                stats->Read(statFileName);
                q = new Query(stats);

                q->QueryPlan();
                if (flag_exec == true) {
                    q->execute();
                    cout << "\n I'm executing your SQL Query";
                    if(op_stdout == true)
                        printQueryOutput(q);
                    else
                        printQueryOutputToFile(q);
                }
                q->cleanup();
                delete q;
                delete stats;
                break;

            case 6:
                cout << "\n BYE, exiting";
                if (dbfile != NULL)
                    dbfile->Close();
                // resetOutput();
                exit(0);
                break;

            default:
                cout << "\n In default!";
                cout << "\n Command Flag: " << commandFlag;
        }

        yy_delete_buffer(buf);
        commandFlag = -1;

        cout << endl;
    }
}