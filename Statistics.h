#ifndef STATISTICS_
#define STATISTICS_
#include <string>
#include <unordered_map>
#include <utility>
#include <string.h>
#include <unistd.h>
#include <iostream>
#include <stdlib.h>
#include "ParseTree.h"

using namespace std;

class RelStats {

public:
    unordered_map<string, int> umap;
    int num_tuples = 0;

   public:
    RelStats();
    void UpdateNumTuples(int num_tuples);
    void AddAtt(char *attName, int numDistincts);
    void Copy(RelStats &toMe);
    void Write(FILE *file);
};

class Statistics {
    friend class RelStats;

   public:
    unordered_map<string, RelStats*> umap;

   public:
    Statistics();
    Statistics(Statistics &copyMe);  // Performs deep copy
    ~Statistics();

    void AddRel(char *relName, int numTuples);
    void AddAtt(char *relName, char *attName, int numDistincts);
    void CopyRel(char *oldName, char *newName);

    void Read(char *fromWhere);
    void Write(char *toWhere);

    void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
};

#endif
