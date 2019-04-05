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
}
Statistics::~Statistics() {
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
}

void Statistics::CopyRel(char *oldName, char *newName) {
    RelStats *old = umap[string(oldName)];
    RelStats *newRel = new RelStats();
    old->Copy(*newRel);
    umap.insert({string(newName), newRel});
}

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
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
}
