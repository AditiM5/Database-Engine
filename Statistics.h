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
    double num_tuples = 0;

   public:
    RelStats();
    void UpdateNumTuples(int num_tuples);
    void AddAtt(char *attName, int numDistincts);
    void Copy(RelStats &toMe, char *newName);
    void Write(FILE *file);
};

class Statistics {
    friend class RelStats;

   public:
    unordered_map<string, RelStats*> umap;
    
    // this lookmap will have all atribute's of all relations distinct values in one map
    unordered_map<string, int> distinct_lookup;

    // this maps the attribute to the relation's total tuples
    // lineitem : l_orderkey - 1000 : where 1000 is the total tuples in lineitem relation
    unordered_map<string, int> numtuples_lookup;

    // this maps the attribute names to the relation 
    unordered_map<string, string> reverse_lookup;

   public:
    Statistics();
    Statistics(Statistics &copyMe);  // Performs deep copy
    ~Statistics();

    void AddRel(char *relName, int numTuples);
    void AddAtt(char *relName, char *attName, int numDistincts);
    void CopyRel(char *oldName, char *newName);

    void Read(char *fromWhere);
    void Write(char *toWhere);

    void JoinRels(string relNames[], double join_result);
    void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
};

#endif
