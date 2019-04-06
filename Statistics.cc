#include "Statistics.h"

// using namespace std;

void RelStats::UpdateNumTuples(int num_tuples) {
    this->num_tuples += num_tuples;
}

RelStats::RelStats() {
    this->num_tuples = 0;
}

void RelStats::AddAtt(char *attName, int numDistincts) {
    string att(attName);
    if (numDistincts == -1) {
        numDistincts = this->num_tuples;
    }

    if (umap.find(att) == umap.end()) {
        umap.insert({att, numDistincts});
    } else {
        int temp = umap[att];
        temp += numDistincts;
        umap[att] = temp;
    }
}

void RelStats::Copy(RelStats &toMe) {
    toMe.num_tuples = this->num_tuples;
    for (pair<string, int> element : umap) {
        toMe.umap.insert(element);
    }
}

void RelStats::Write(FILE *file) {
    for (pair<string, int> element : umap) {
        fprintf(file, "%s %d\n", element.first.c_str(), element.second);
    }
}

Statistics::Statistics() {
}

Statistics::Statistics(Statistics &copyMe) {
    for (pair<string, RelStats *> element : copyMe.umap) {
        RelStats *relstat = new RelStats();
        element.second->Copy(*relstat);
        this->umap.insert({element.first, relstat});
    }
}
Statistics::~Statistics() {
    //empty
    for (pair<string, RelStats *> element : this->umap) {
        delete element.second;
    }
    umap.clear();
}

void Statistics::AddRel(char *relName, int numTuples) {
    string rel = string(relName);
    if (umap.find(rel) == umap.end()) {
        RelStats *temp = new RelStats();
        temp->UpdateNumTuples(numTuples);
        umap.insert({rel, temp});
    } else {
        RelStats *relstats = umap[rel];
        relstats->UpdateNumTuples(numTuples);
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    string rel(relName);
    RelStats *temp = umap[rel];
    temp->AddAtt(attName, numDistincts);

    //update distinct_lookup
    distinct_lookup.insert({string(attName), numDistincts});

    //update numtuples_lookup
    numtuples_lookup.insert({string(attName), umap[rel]->num_tuples});
}

//TODO: fix this
void Statistics::CopyRel(char *oldName, char *newName) {
    RelStats *old = umap[string(oldName)];
    RelStats *newRel = new RelStats();
    old->Copy(*newRel);
    umap.insert({string(newName), newRel});
}

//TODO: fix this
void Statistics::Read(char *fromWhere) {
    FILE *stat_file = fopen(fromWhere, "r");

    char space[200];

    while (fscanf(stat_file, "%s", space) != EOF) {
        if (strcmp(space, "BEGIN")) {
            cout << "Error! Not a stat file!" << endl;
            exit(1);
        }

        fscanf(stat_file, "%s", space);
        string relName(space);
        int *numtuples = new int;
        fscanf(stat_file, "%d", numtuples);
        RelStats *relStats = new RelStats();
        relStats->num_tuples = *numtuples;
        fscanf(stat_file, "%s", space);

        while (strcmp(space, "END")) {
            int *numDistinct = new int;
            fscanf(stat_file, "%d", numDistinct);
            relStats->AddAtt(space, *numDistinct);
            fscanf(stat_file, "%s", space);
        }
        umap.insert({relName, relStats});
    }
}

void Statistics::Write(char *toWhere) {
    FILE *stat_file = fopen(toWhere, "w");
    for (pair<string, RelStats *> element : umap) {
        fprintf(stat_file, "%s\n", "BEGIN");
        fprintf(stat_file, "%s %d\n", element.first.c_str(), element.second->num_tuples);
        element.second->Write(stat_file);
        fprintf(stat_file, "%s\n\n", "END");
    }
    fclose(stat_file);
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    bool isJoin = false;
    double factor = 1;
    double result = 1;
    int count = 0;

    while (parseTree != NULL) {
        struct OrList *left = parseTree->left;

        struct ComparisonOp *cop = left->left;

        struct Operand *right_cop = cop->right;
        string right_key = string(right_cop->value);
        // double right_tuple = numtuples_lookup[right_key];
        // RelStats *right_rel = NULL;

        struct Operand *left_cop = cop->left;
        string left_key = string(left_cop->value);
        double left_tuple = numtuples_lookup[left_key];
        cout << "Stage A" << endl;
        cout << "Stage 1: left_tuple: " << left_tuple << endl;
        // RelStats *left_rel = NULL;

        factor = 1;

        while (left != NULL) {
            // if (left_rel == NULL) {
            //     cout << "Relation not found!" << endl;
            //     exit(1);
            // }

            // string left_key = string(left_cop->value);
            cop = left->left;
            right_cop = cop->right;
            left_cop = cop->left;
            left_key = string(left_cop->value);
            right_key = string(right_cop->value);

            cout << "Stage B" << endl;
            if (right_cop->code == 4) {
                // then it is JOIN
                isJoin = true;
                double right_tuple = numtuples_lookup[right_key];
                cout << "Stage 2: right_tuple" << right_tuple << endl;
                cout << "Stage C" << endl;

            } else {
                cout << "Stage D" << endl;
                // it is a selection
                isJoin = false;
                cout << "Cop code: " << cop->code << endl;

                if (cop->code == 3) {
                    cout << "Stage E" << endl;
                    // means EQUALS operator
                    double left_distinct = distinct_lookup[left_key];
                    cout << "Stage 3: left_distinct " << left_distinct << endl;
                    // double m = (double)result / left_distinct;
                    factor *= (1 - ((double)1 / left_distinct));
                } else {
                    cout << "Stage F" << endl;
                    // means <, > operators
                    cout << "Not equals" << endl;
                    factor *= ((double)2 / 3);
                }
            }
            cout << "Factor: " << factor << endl;

            left = left->rightOr;

            //counting for every disjunction (AND)
            count++;
            cout << "Count: " << count << endl;
        }
        // cout << "Count: " << count << endl;

        parseTree = parseTree->rightAnd;

        if (count == 1) {
            result = (left_tuple * (1 - factor));
        } else {
            cout << "Calculating result" << endl;
            result = result * (1 - factor);
        }
        // left_tuple = result;
        cout << "Left Tuple: " << left_tuple << endl;

        cout << "Result: " << result << endl;
    }
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
}
