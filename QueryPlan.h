#include <iostream>
#include "QueryPlanUtils.h"
#include "RelOp.h"
#include "Statistics.h"
#define PIPE_SIZE 100

using namespace std;

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

class GenericNode {
   public:
    Pipe *outputPipe = new Pipe(PIPE_SIZE);
    GenericNode *lChild = NULL;
    GenericNode *rChild = NULL;
    Schema *outschema;
    virtual void execute() = 0;
    virtual void WaitUntilDone() = 0;
    virtual void Print() = 0;
};

class SelectPipeNode : public GenericNode {
   public:
    SelectPipe selectPipe;
    CNF cnf;
    Record literal;
    Pipe *in = new Pipe(PIPE_SIZE);

    SelectPipeNode(Pipe *in, Schema *schema, struct AndList *curr, GenericNode *root) {
        cout << "\n SelectPipeNode...";
        this->in = in;
        this->outschema = schema;

        removeMapping(curr->left);
        cout << "\n Printing orList: ";
        printOrList(curr->left);
        cnf.GrowFromParseTree(curr, schema, literal);

        // left deep blah
        if (root != NULL) {
            this->lChild = root;
        }
    }

    void execute() {
        selectPipe.Run(*in, *outputPipe, cnf, literal);
    }

    void WaitUntilDone() {
        selectPipe.WaitUntilDone();
    }

    void Print() {
        cout << "\n SelectPipeNode: ";
        cnf.Print();
    }
};

class SelectFileNode : public GenericNode {
   public:
    SelectFile selectFile;
    DBFile *file;
    CNF cnf;
    Record literal;

    SelectFileNode(DBFile *dbfile, Schema *schema, struct AndList *curr) {
        if (curr == NULL) {
            curr = new AndList;
            curr->left = NULL;
            curr->rightAnd = NULL;
        } else {
            removeMapping(curr->left);
        }

        this->file = dbfile;
        outschema = schema;
        cnf.GrowFromParseTree(curr, schema, literal);
    }

    void execute() {
        selectFile.Run(*file, *outputPipe, cnf, literal);
    }

    void WaitUntilDone() {
        selectFile.WaitUntilDone();
    }

    void Print() {
        cout << "\n SelectFileNode: ";
        cnf.Print();
    }
};

class ProjectNode : public GenericNode {
   public:
    Project project;

    // variables that will be passed to the Project object in
    // ProjectNode.Run()
    int *keepMe;
    int numAttsIn, numAttsOut;
    Pipe *inputPipe;

    ProjectNode(NameList *atts, GenericNode *&root) {
        lChild = root;
        root = this;
        inputPipe = lChild->outputPipe;

        NameList tempNameList = *atts;  // create a temporary copy of atts because it will be destroyed
                                        // when we walk it to determine atts below

        // init the schema
        Schema *inschema = lChild->outschema;

        numAttsIn = inschema->GetNumAtts();

        // init variables that will be passed to the Project object in
        // ProjectNode.Run()
        vector<int> temp;
        vector<Type> tempType;    // will be used below to prune inschema
        vector<char *> tempName;  // will be used below to prune inschema
        NameList *tempL = &tempNameList;
        do {
            // if the input attribute has the relation name prepended along with a dot,
            // remove it
            char *dotStripped = strpbrk(tempL->name, ".") + 1;
            if (dotStripped != NULL) {  // there was a dot and we removed the prefix
                tempL->name = dotStripped;
                temp.push_back(inschema->Find(dotStripped));
                tempType.push_back(inschema->FindType(dotStripped));
                tempName.push_back(dotStripped);
            } else {  // there was no dot; use the name as-is
                temp.push_back(inschema->Find(tempL->name));
                tempType.push_back(inschema->FindType(tempL->name));
                tempName.push_back(tempL->name);
            }
            tempL = tempL->next;
        } while (tempL);
        numAttsOut = temp.size();
        keepMe = new int[numAttsOut]();
        Attribute *tempAttArray = new Attribute[numAttsOut]();  // will be used below to prune inschema
        for (int i = 0; i < numAttsOut; i++) {
            keepMe[i] = temp.at(i);
            tempAttArray[i].name = tempName.at(i);
            tempAttArray[i].myType = tempType.at(i);
        }

        // Prune the input schema to contain only the attributes that we are projecting upon
        // Save it as ProjectNode's rschema so that it can be used to parse the output in clear_pipe
        outschema = new Schema("projectOutputSchema", numAttsOut, tempAttArray);
    }

    void execute() {
        project.Run(*inputPipe, *outputPipe, keepMe, numAttsIn, numAttsOut);
    }

    void WaitUntilDone() {
        project.WaitUntilDone();
    }

    void Print() {
        cout << "\n ProjectNode : ";
        printOutputSchema(outschema);
    }
};
class JoinNode : public GenericNode {
   public:
    Pipe *left;
    Pipe *right;
    CNF cnf;
    Record literal;
    Join join;
    Schema *lSchema, *rSchema;

    JoinNode(struct AndList *curr, string rel1, string rel2, unordered_map<string, GenericNode *> &relToNode) {
        cout << "\n IN Join Node...";
        cout << "\n GrowFromParseTree debug";
        cout << "\n rel1: " << rel1;
        cout << "\n rel2: " << rel2;
        GenericNode *lNode = NULL;
        GenericNode *rNode = NULL;

        cout << "\n RelToNode: ";
        cout << "\n count rel1: " << relToNode.count(rel1);
        cout << "\n count rel2: " << relToNode.count(rel2);

        cout << "\n Hello Assholes";
        cout << "\n Hello Assholes";
        cout << "\n Hello Assholes";
        cout << "\n Hello Assholes";
        cout << "\n Hello Assholes";

        for (pair<string, GenericNode *> ele : relToNode) {
            cout << "\n Key: " << ele.first;
        }

        if (relToNode.count(rel1) != 0 && relToNode.count(rel2) != 0) {
            cout << "\n Join Stage 1";
            // both rels already exist
            lNode = relToNode[rel1];
            rNode = relToNode[rel2];
        } else if (relToNode.count(rel1) != 0 && relToNode.count(rel2) == 0) {
            cout << "\n Join Stage 2";
            // first exists second does not exist
            lNode = relToNode[rel1];

            Schema *myschema = new Schema("catalog", rel2.c_str());
            cout << "\n Schema: ";
            printOutputSchema(myschema);
            DBFile *dbfile = new DBFile();
            const char *dir = ("data/" + rel2 + ".bin").c_str();
            dbfile->Open(dir);
            dbfile->MoveFirst();

            rNode = new SelectFileNode(dbfile, myschema, NULL);
            relToNode[rel2] = rNode;

        } else if (relToNode.count(rel1) == 0 && relToNode.count(rel2) != 0) {
            cout << "\n Join Stage 3";
            // first does not exist, second does

            rNode = relToNode[rel2];

            Schema *myschema = new Schema("catalog", rel1.c_str());
            DBFile *dbfile = new DBFile();
            const char *dir = ("data/" + rel1 + ".bin").c_str();
            dbfile->Open(dir);
            dbfile->MoveFirst();

            lNode = new SelectFileNode(dbfile, myschema, NULL);
            relToNode[rel1] = lNode;

        } else if (relToNode.count(rel1) == 0 && relToNode.count(rel2) == 0) {
            cout << "\n Join Stage 4";
            // both do not exist

            // for left node
            Schema *myschema = new Schema("catalog", rel1.c_str());
            DBFile *dbfile = new DBFile();
            const char *dir = ("data/" + rel1 + ".bin").c_str();
            dbfile->Open(dir);
            dbfile->MoveFirst();
            lNode = new SelectFileNode(dbfile, myschema, NULL);
            relToNode[rel1] = lNode;

            // for right node
            myschema = new Schema("catalog", rel2.c_str());
            dbfile = new DBFile();
            dir = ("data/" + rel2 + ".bin").c_str();
            dbfile->Open(dir);
            dbfile->MoveFirst();
            rNode = new SelectFileNode(dbfile, myschema, NULL);
            relToNode[rel2] = rNode;
        }

        cout << "\n Hello World";
        cout << "\n Hello World";
        cout << "\n Hello World";
        cout << "\n Hello World";

        lSchema = lNode->outschema;
        rSchema = rNode->outschema;
        this->left = lNode->outputPipe;
        this->right = rNode->outputPipe;

        cout << "\n left schema: ";
        printOutputSchema(lSchema);

        cout << "\n right schema: ";
        printOutputSchema(rSchema);

        // change mappings for rel1 , rel2 and for a new one called rel1_rel2
        this->lChild = relToNode[rel1];
        this->rChild = relToNode[rel2];
        relToNode[rel1] = this;
        relToNode[rel2] = this;
        string joinTemp = rel1 + "_" + rel2;
        cout << "\n joinTemp: " << joinTemp;
        relToNode[joinTemp] = this;

        cout << "\n JOIN OrList";
        printOrList(curr->left);
        removeMapping(curr->left);
        cnf.GrowFromParseTree(curr, lSchema, rSchema, literal);

        cout << "\n Debugging outschema...";
        // create the new outschema which will have atts of both left and right
        int numAtts = lSchema->GetNumAtts() + rSchema->GetNumAtts();
        cout << "\n NumAtts: " << numAtts;
        struct Attribute *mergeAtts = new Attribute[numAtts];
        struct Attribute *atts1 = lSchema->GetAtts();
        struct Attribute *atts2 = rSchema->GetAtts();

        int k = 0;  // index for mergeAtts
        for (int i = 0; i < lSchema->GetNumAtts(); i++) {
            mergeAtts[k].myType = atts1[i].myType;
            mergeAtts[k].name = strdup(atts1[i].name);
            cout << "\n k: " << k << ": " << string(atts1[i].name);
            k++;
        }

        for (int i = 0; i < rSchema->GetNumAtts(); i++) {
            mergeAtts[k].myType = atts2[i].myType;
            mergeAtts[k].name = strdup(atts2[i].name);
            cout << "\n k: " << k << ": " << string(atts2[i].name);
            k++;
        }

        // now k = size;
        char *join_name = &(rel1 + rel2)[0u];
        outschema = new Schema(join_name, numAtts, mergeAtts);
    }

    void execute() {
        join.Run(*left, *right, *outputPipe, cnf, literal);
    }

    void WaitUntilDone() {
        join.WaitUntilDone();
    }

    void Print() {
        cout << "\n Join Node: ";
        // cout << "\n Left: ";
        // printOutputSchema(lSchema);
        // cout << "\n Right: ";
        printOutputSchema(outschema);
    }
};

class DistinctNode : public GenericNode {
   public:
    Pipe *inputPipe;
    Schema *inSchema;
    DuplicateRemoval duprem;

    DistinctNode(GenericNode *root, struct NameList *attsToSelect) {
        cout << "\n Distinct Stage 1";
        inputPipe = root->outputPipe;
        outschema = root->outschema;

        Attribute *atts = new Attribute[outschema->GetNumAtts()];

        int numatts = 0;
        cout << "\n Distinct Stage 2";
        while (attsToSelect != NULL) {
            searchAtt(attsToSelect->name, outschema, (atts + numatts));
            numatts++;
            attsToSelect = attsToSelect->next;
        }
        cout << "\n Distinct Stage 3";
        char *filename = "DistinctSchema";
        inSchema = new Schema(filename, numatts, atts);
    }

    void execute() {
        duprem.Run(*inputPipe, *outputPipe, *inSchema);
    }

    void WaitUntilDone() {
        duprem.WaitUntilDone();
    }

    void Print() {
        cout << "\n Duplicate Removal Node: ";
        printOutputSchema(outschema);
    }
};

class SumNode : public GenericNode {
};

class GroupByNode : public GenericNode {
    OrderMaker *groupAtts;
};

class FunctionNode : public GenericNode {
   public:
    Pipe *inputpipe;
    Function func;
    Sum sum;

    FunctionNode(GenericNode *root, FuncOperator *finalFunction) {
        inputpipe = root->outputPipe;
        func.GrowFromParseTree(finalFunction, *(root->outschema));
    }

    void execute() {
        sum.Run(*inputpipe, *outputPipe, func);
    }

    void WaitUntilDone() {
        sum.WaitUntilDone();
    }

    void Print() {
        cout << "\n Function Node";
        func.Print();
    }
};

class Query {
   public:
    Statistics *stats;
    GenericNode *root;
    // maps TableName / relation Name to the node
    unordered_map<string, GenericNode *> relToNode;
    // Alias to Relation Name mapping
    unordered_map<string, string> aliasToRel;

    void QueryPlan();
    void AndListEval(struct AndList *candidates);
    GenericNode *CreateTreeNode(struct AndList *curr, int numToJoin);
    void postOrderPrint(GenericNode *root);

    Query(Statistics *stats) {
        this->stats = stats;
    }
};

GenericNode *Query ::CreateTreeNode(struct AndList *curr, int numToJoin) {
    // get the left relation and attr name
    string temp = curr->left->left->left->code != 4 ? string(curr->left->left->left->value) : string(curr->left->left->right->value);
    string rel1 = temp.substr(0, temp.find("."));
    string rel1Attr = temp.substr(temp.find(".") + 1, string::npos);

    if (aliasToRel.count(rel1)) {
        rel1 = aliasToRel[rel1];
    }
    cout << "\n Rel 1 Actual: " << rel1;
    cout << "\n Rel1 Attr: " << rel1Attr << "\n";

    GenericNode *node;

    cout << "\n NumToJoin: " << numToJoin;

    if (numToJoin == 1) {
        cout << "\n It's a selection";
        // it is a selection
        if (relToNode.count(rel1) != 0) {
            cout << "\n Stage CreateNode 1";
            // make a selectPipe node
            // get schema from the outpipe of whatever executed earlier of that relation
            Schema *inSchema = relToNode[rel1]->outschema;
            // now this will become schema for the new Node
            node = new SelectPipeNode(relToNode[rel1]->outputPipe, inSchema, curr, relToNode[rel1]);

        } else {
            cout << "\n Stage CreateNode 2";
            // make a selectFile node
            // new - get schema from Catalog
            Schema *myschema = new Schema("catalog", rel1.c_str());
            DBFile *dbfile = new DBFile();
            cout << "\n Stage CreateNode 3";
            const char *dir = ("data/" + rel1 + ".bin").c_str();
            cout << "\n Stage CreateNode 4";
            cout << "\n Stage CreateNode 4";
            cout << "\n Stage CreateNode 4";
            cout << "\n Stage CreateNode 4";
            dbfile->Open(dir);
            cout << "\n Stage CreateNode 5";
            dbfile->MoveFirst();
            cout << "\n Stage CreateNode 6";
            node = new SelectFileNode(dbfile, myschema, curr);
            cout << "\n Stage CreateNode 7";
        }

        cout << "\n Stage CreateNode 8";
        // here add to the map
        relToNode[rel1] = node;
    } else if (numToJoin == 2) {
        cout << "\n Stage CreateNode 9";
        // then it is a join
        // get the left relation and attr name
        temp = string(curr->left->left->right->value);
        cout << "\n temp string: " << temp;
        string rel2 = temp.substr(0, temp.find("."));
        string rel2Attr = temp.substr(temp.find(".") + 1, temp.size());

        cout << "\n Rel2: " << rel2;
        if (aliasToRel.count(rel2)) {
            rel2 = aliasToRel[rel2];
        }
        cout << "\n Rel2 Attr: " << rel2Attr;

        node = new JoinNode(curr, rel1, rel2, relToNode);
    }

    cout << "\n End of CreateNode!!!";
    return node;
}

void Query::AndListEval(struct AndList *candidates) {
    struct AndList *andList = candidates;
    struct AndList *currAndList;

    if (!candidates) {
        return;
    }

    // stores the cost of each AndList Conjunction's Permutation
    vector<double> estimates;

    // string for the Relation Names
    string rel1;
    string rel2;
    int numToJoin = 1;

    while (andList) {
        numToJoin = 1;
        currAndList = new AndList;
        currAndList->left = andList->left;
        currAndList->rightAnd = NULL;

        cout << "\n Before calling estimate...";
        printAndList(currAndList);
        double currEst = stats->Estimate(currAndList, NULL, 0);
        cout << "\n Estimating curr AndList: " << currEst;
        estimates.push_back(currEst);
        andList = andList->rightAnd;
    }

    cout << "\n Stage 1";
    // now select the AndList of the min Cost
    int minIndex = 0;
    double temp = estimates[0];
    for (int i = 0; i < estimates.size(); i++) {
        if (estimates[i] < temp) {
            minIndex = i;
            temp = estimates[i];
        }
    }

    cout << "\n Stage 2";
    // the andList with the minimum cost is the one that should go in first into the sofar list
    // get the Andlist element based on the minInd, pluck it out and insert it into the sofar list.
    struct AndList *target = candidates;
    struct AndList *pre = candidates;

    // this while loop find the right node to perform the operation. toggle the list node around.
    while (minIndex > 0) {
        minIndex--;
        pre = target;
        target = target->rightAnd;
    }

    cout << "\n Stage 3";
    if (target == candidates) {  // the node to be removed is the first node.
        candidates = target->rightAnd;
        target->rightAnd = NULL;
    } else {  // the node to be removed is in the middle or tail.
        pre->rightAnd = target->rightAnd;
        target->rightAnd = NULL;
    }

    cout << "\n Stage 4";
    currAndList->left = target->left;
    cout << "\n Stage 4.5";
    stats->Apply(target, NULL, 1);

    cout << "\n Stage 5";

    struct OrList *left = target->left;
    struct ComparisonOp *compLeft = left->left;
    if (compLeft->left->code == NAME && compLeft->right->code == NAME) {
        // rel1 = string(compLeft->left->value);
        // rel2 = string(compLeft->right->value);
        cout << "\n I am setting numToJoin as JOIN";
        numToJoin = 2;
    } else {
        // it is a selection, either the first or the second is a attsName
        cout << "\n I am setting numToJoin as SELECTION";
        numToJoin = 1;
    }

    // now make a treeNode out of the current AndList
    // set that as the root node
    // the last one to be set will be the largest costing AndList
    root = CreateTreeNode(target, numToJoin);

    cout << "\n Stage 6";
    // recurse to get the next minimum AndList
    AndListEval(candidates);
}

void getAttsFromFunc(struct FuncOperator *finalFunction, struct NameList *atts, struct NameList *head) {
    cout << "\n Code: " << finalFunction->code;

    if (finalFunction->leftOperand != NULL) {
        cout << "\n Left Operand Code: " << (finalFunction->leftOperand->code);
        cout << "\n Left Operand Value: " << string(finalFunction->leftOperand->value);
        struct NameList *temp;
        if (head == NULL) {
            head = new NameList;
            head->name = strdup(finalFunction->leftOperand->value);
            atts = head;
        } else {
            temp = new NameList;
            temp->name = strdup(finalFunction->leftOperand->value);
            atts->next = temp;
            atts = temp;
        }
    }

    if (finalFunction->leftOperator != NULL) {
        cout << "\n Left Operator: ";
        getAttsFromFunc(finalFunction->leftOperator, atts, head);
    }

    if (finalFunction->right != NULL) {
        cout << "\n Right Operator: ";
        getAttsFromFunc(finalFunction->right, atts, head);
    }

    cout << endl;
}

void Query ::QueryPlan() {
    int numRels = 0;
    TableList *t = tables;

    while (t) {
        numRels++;
        if (t->aliasAs) {
            stats->CopyRel(t->tableName, t->aliasAs);
            aliasToRel[string(t->aliasAs)] = string(t->tableName);
        }
        t = t->next;
    }

    stats->Write("debug_stats.txt");
    AndListEval(boolean);

    // after here you should have a root

    // check for non aggr distinct
    cout << "\n Stage: Distinct";
    if (distinctAtts == 1) {
        DistinctNode *dn = new DistinctNode(root, attsToSelect);
        dn->lChild = root;
        root = dn;
    }

    if (distinctFunc == 1) {
        struct NameList *atts, *head;
        getAttsFromFunc(finalFunction, atts, head);
        DistinctNode *dn = new DistinctNode(root, head);
        dn->lChild = root;
        root = dn;

        FunctionNode *fn = new FunctionNode(root, finalFunction);
        fn->lChild = root;
        root = fn;
        cout << "\n Distinct func set";
    } else if (finalFunction != NULL) {
        cleanFuncOperator(finalFunction);
        FunctionNode *fn = new FunctionNode(root, finalFunction);
        fn->lChild = root;
        root = fn;
    }

    // for projections
    if (attsToSelect) {
        ProjectNode(attsToSelect, root);
    }

    cout << "\n Printing the current tree: ";
    postOrderPrint(root);
}

/*------------------------------------------------------------------------------
 * Print() EVERY node whilst traversing the tree post-order
 *----------------------------------------------------------------------------*/
void Query::postOrderPrint(GenericNode *currentNode) {
    if (!currentNode) {
        return;
    }
    postOrderPrint(currentNode->lChild);
    postOrderPrint(currentNode->rChild);
    currentNode->Print();
    // cout<< "\n Node";
    //   currentNode->Run();
}