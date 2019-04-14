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

void RelStats::Copy(RelStats &toMe, char *newName) {
    toMe.num_tuples = this->num_tuples;
    for (pair<string, int> element : umap) {
        string temp = newName != NULL ? string(newName) + "." + element.first : element.first;
        toMe.umap.insert({temp, element.second});
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
        element.second->Copy(*relstat, NULL);
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

    // update the reverse_lookup
    reverse_lookup.insert({string(attName), rel});
}

void Statistics::CopyRel(char *oldName, char *newName) {
    RelStats *old = umap[string(oldName)];
    RelStats *newRel = new RelStats();
    old->Copy(*newRel, newName);
    umap.insert({string(newName), newRel});

    // update all lookups
    for (pair<string, int> element : newRel->umap) {
        distinct_lookup.insert(element);
        numtuples_lookup.insert({element.first, newRel->num_tuples});
        reverse_lookup.insert({element.first, string(newName)});
    }
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

        for (pair<string, int> element : relStats->umap) {
            distinct_lookup.insert(element);
            numtuples_lookup.insert({element.first, relStats->num_tuples});
            reverse_lookup.insert({element.first, relName});
        } 
    }

}

void Statistics::Write(char *toWhere) {
    FILE *stat_file = fopen(toWhere, "w");
    for (pair<string, RelStats *> element : umap) {
        fprintf(stat_file, "%s\n", "BEGIN");
        cout << "Result:  IN WRITE" << element.second->num_tuples << endl;
        fprintf(stat_file, "%s %g\n", element.first.c_str(), element.second->num_tuples);
        element.second->Write(stat_file);
        fprintf(stat_file, "%s\n\n", "END");
    }
    fclose(stat_file);
}

void Statistics::JoinRels(string relNames[], double join_result) {
    cout << "IN Join Rels" << endl;
    cout << "First rel: " << relNames[0] << endl;
    cout << "second rel: " << string(relNames[1]) << endl;

    RelStats *left_rel = umap[string(relNames[0])];

    cout << "Hello there" << endl;
    RelStats *right_rel = umap[string(relNames[1])];

    left_rel->num_tuples = join_result;

    string newRelName = string(relNames[0]) + "_" + string(relNames[1]);

    cout << "Stage : 1" << endl;

    for (pair<string, int> element : right_rel->umap) {
        left_rel->umap.insert(element);
    }
    cout << "Stage : 2" << endl;

    for (pair<string, int> element : left_rel->umap) {
        auto it1 = reverse_lookup.find(element.first);
        it1->second = newRelName;

        auto it2 = numtuples_lookup.find(element.first);
        it2->second = join_result;
    }

    cout << "Stage : 3" << endl;

    auto it = umap.find(string(relNames[0]));
    swap(umap[newRelName], it->second);
    umap.erase(it);

    cout << "Stage : 4" << endl;
    umap.erase(string(relNames[1]));
    cout << "End of JOin rels" << endl;
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    bool isJoin = false;
    double factor = 1;
    double result = 1;
    int count = 0;

    while (parseTree != NULL) {
        cout << "Stage A" << endl;
        struct OrList *left = parseTree->left;
        struct ComparisonOp *cop = left->left;

        cout << "Stage B" << endl;
        struct Operand *right_cop = cop->right;
        string right_key = string(right_cop->value);

        cout << "Stage C" << endl;
        struct Operand *left_cop = cop->left;
        string left_key = string(left_cop->value);
        double left_tuple = numtuples_lookup[left_key];

        result = left_tuple;
        cout << "left_tuple: " << result << endl;

        cout << "Stage D" << endl;

        factor = 1;
        // current relation on which selection is done
        string sel_relname;

        while (left != NULL) {
            // left
            cout << "Left" << endl;
            cop = left->left;
            left_cop = cop->left;
            left_key = string(left_cop->value);
            double left_distinct = distinct_lookup[left_key];

            // right
            cout << "Right" << endl;
            right_cop = cop->right;
            right_key = string(right_cop->value);

            if (right_cop->code == 4) {
                cout << "Starting Join... " << endl;
                isJoin = true;
                double right_tuple = numtuples_lookup[right_key];
                double right_distinct = distinct_lookup[right_key];
                double join_result = left_tuple * right_tuple / max(left_distinct, right_distinct);

                string rel_names[] = {reverse_lookup[left_key], reverse_lookup[right_key]};

                cout << "Join rels.. " << endl;
                JoinRels(rel_names, join_result);

            } else {
                // it is a selection
                isJoin = false;
                cout << "Sel _ rel names " << endl;
                cout << "left_key: " << left_key << endl;
                sel_relname = reverse_lookup[left_key];

                if (cop->code == 3) {
                    // means EQUALS operator
                    // double left_distinct = distinct_lookup[left_key];
                    factor *= (1 - ((double)1 / left_distinct));
                } else {
                    // means <, > operators
                    factor *= ((double)2 / 3);
                }
            }

            cout << "Stage E" << endl;
            left = left->rightOr;

            //counting for every disjunction (AND)
            count++;
        }
        parseTree = parseTree->rightAnd;

        cout << "Stage F" << endl;
        // if (count == 1) {
        //     result = (left_tuple * (1 - factor));
        // } else {
        // }

        cout << "Stage G" << endl;
        cout << "Result : " << result << endl;
        if (!isJoin) {
            result = result * (1 - factor);
            cout << "sel_relname: " << sel_relname << endl;
            RelStats *temp = umap[sel_relname];
            temp->num_tuples = result;

            for (pair<string, int> element : umap[sel_relname]->umap) {
                numtuples_lookup[element.first] = result;
                cout << "Updating lookups with: " << result << endl;
            }
        }
        cout << "Final result: " << result;
    }
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
    unordered_map<string, RelStats*> temp_umap;
    unordered_map<string, int> temp_distinct_lookup;
    unordered_map<string, int> temp_numtuples_lookup;
    unordered_map<string, string> temp_reverse_lookup;

    for (pair<string, RelStats*> element : umap) {
        RelStats *temprel = new RelStats();
        // char *tempstring = strdup(element.first.c_str());
        element.second->Copy(*temprel, NULL);
        temp_umap.insert({element.first, temprel});
    }

    for (pair<string, int> element : distinct_lookup) {
        temp_distinct_lookup.insert(element);
    }

    for (pair<string, int> element : numtuples_lookup) {
        
        temp_numtuples_lookup.insert(element);
    }

    for (pair<string, string> element : reverse_lookup) {
        cout << "1: " << element.first << " 2: " << element.second << endl;
        temp_reverse_lookup.insert(element);
    }


    bool isJoin = false;
    double factor = 1;
    double result = 1;
    double join_result = 0;
    int count = 0;

    while (parseTree != NULL) {
        cout << "Stage A" << endl;
        struct OrList *left = parseTree->left;
        struct ComparisonOp *cop = left->left;

        cout << "Stage B" << endl;
        struct Operand *right_cop = cop->right;
        string right_key = string(right_cop->value);

        cout << "Stage C" << endl;
        struct Operand *left_cop = cop->left;
        string left_key = string(left_cop->value);
        cout << "Looking up left key: " << left_key << endl;
        double left_tuple = temp_numtuples_lookup[left_key];

        result = left_tuple;
        cout << "left_tuple: " << result << endl;

        cout << "Stage D" << endl;

        factor = 1;
        // current relation on which selection is done
        string sel_relname;

        while (left != NULL) {
            // left
            cout << "Left" << endl;
            cop = left->left;
            left_cop = cop->left;
            left_key = string(left_cop->value);
            double left_distinct = temp_distinct_lookup[left_key];

            // right
            cout << "Right" << endl;
            right_cop = cop->right;
            right_key = string(right_cop->value);

            if (right_cop->code == 4) {
                cout << "Starting Join... " << endl;
                isJoin = true;
                double right_tuple = temp_numtuples_lookup[right_key];
                double right_distinct = temp_distinct_lookup[right_key];
                join_result = left_tuple * right_tuple / max(left_distinct, right_distinct);

                string rel_names[] = {temp_reverse_lookup[left_key], temp_reverse_lookup[right_key]};

                cout << "Join rels.. " << endl;
                
                cout << "IN Join Rels" << endl;
                cout << "First rel: " << rel_names[0] << endl;
                cout << "second rel: " << string(rel_names[1]) << endl;

                RelStats *left_rel = temp_umap[string(rel_names[0])];

                cout << "Hello there" << endl;
                RelStats *right_rel = temp_umap[string(rel_names[1])];

                left_rel->num_tuples = join_result;

                string newRelName = string(rel_names[0]) + "_" + string(rel_names[1]);

                cout << "Stage : 1" << endl;

                for (pair<string, int> element : right_rel->umap) {
                    left_rel->umap.insert(element);
                }
                cout << "Stage : 2" << endl;

                for (pair<string, int> element : left_rel->umap) {
                    auto it1 = temp_reverse_lookup.find(element.first);
                    it1->second = newRelName;

                    auto it2 = temp_numtuples_lookup.find(element.first);
                    it2->second = join_result;
                }

                cout << "Stage : 3" << endl;

                auto it = temp_umap.find(string(rel_names[0]));
                swap(temp_umap[newRelName], it->second);
                temp_umap.erase(it);

                cout << "Stage : 4" << endl;
                temp_umap.erase(string(rel_names[1]));
                cout << "End of JOin rels" << endl;

            } else {
                // it is a selection
                isJoin = false;
                cout << "Sel _ rel names " << endl;
                cout << "left_key: " << left_key << endl;
                sel_relname = temp_reverse_lookup[left_key];

                if (cop->code == 3) {
                    // means EQUALS operator
                    // double left_distinct = distinct_lookup[left_key];
                    factor *= (1 - ((double)1 / left_distinct));
                } else {
                    // means <, > operators
                    factor *= ((double)2 / 3);
                }
            }

            cout << "Stage E" << endl;
            left = left->rightOr;

            //counting for every disjunction (AND)
            count++;
        }
        parseTree = parseTree->rightAnd;

        cout << "Stage F" << endl;
        // if (count == 1) {
        //     result = (left_tuple * (1 - factor));
        // } else {
        // }

        cout << "Stage G" << endl;
        if (!isJoin) {
            result = result * (1 - factor);
            cout << "Result : " << result << endl;
            cout << "sel_relname: " << sel_relname << endl;
            RelStats *temp = temp_umap[sel_relname];
            temp->num_tuples = result;

            // cout << "Relstats numtuples: " << temp->num_tuples <<endl;
            cout << "Umap size : " << temp_umap[sel_relname]->umap.size() << endl;

            for (pair<string, int> element : temp_umap[sel_relname]->umap) {
                cout << "Updating lookups: rel: " << element.first << " result: " << result << endl;
                temp_numtuples_lookup[element.first] = result;
            }
        }
        // cout << "Final result: " << result;
    }

    return isJoin ? join_result : result;
}
