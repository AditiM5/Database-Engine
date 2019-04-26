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

unordered_map<Pipe *, int> inputpipemap;
unordered_map<Pipe *, int> outputpipemap;

// max ID of inpuit and output pipes
int inp = 0, outp = 0;

class GenericNode {
   public:
    Pipe *outputPipe = new Pipe(PIPE_SIZE);
    // input pipes;
    Pipe *leftPipe = NULL, *rightPipe = NULL;
    GenericNode *lChild = NULL;
    GenericNode *rChild = NULL;
    Schema *outschema;
    virtual void execute() = 0;
    virtual void WaitUntilDone() = 0;
    virtual void Print() = 0;
    void printPipeIDs();
};

class SelectPipeNode : public GenericNode {
   public:
    SelectPipe selectPipe;
    CNF cnf;
    Record literal;

    SelectPipeNode(Pipe *in, Schema *schema, struct AndList *curr, GenericNode *root) {
        this->leftPipe = in;
        this->outschema = schema;

        removeMapping(curr->left);
        cnf.GrowFromParseTree(curr, schema, literal);

        // left deep blah
        if (root != NULL) {
            this->lChild = root;
        }
    }

    void execute() {
        selectPipe.Run(*leftPipe, *outputPipe, cnf, literal);
    }

    void WaitUntilDone() {
        selectPipe.WaitUntilDone();
    }

    void Print() {
        cout << "\n SelectPipeNode: ";
        printPipeIDs();
        printOutputSchema(outschema);
        cout << "\n Select CNF: ";
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
        printPipeIDs();
        printOutputSchema(outschema);
        cout << "\n Select CNF: ";
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

    ProjectNode(NameList *atts, GenericNode *&root) {
        leftPipe = root->outputPipe;

        NameList tempNameList = *atts;  // create a temporary copy of atts because it will be destroyed
                                        // when we walk it to determine atts below

        // init the schema
        Schema *inschema = root->outschema;

        numAttsIn = inschema->GetNumAtts();

        // init variables that will be passed to the Project object in
        // ProjectNode.Run()
        vector<int> temp;
        vector<Type> tempType;    // will be used below to prune inschema
        vector<char *> tempName;  // will be used below to prune inschema
        NameList *tempL = &tempNameList;

        //TODO: Replace with removeDot()
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
        project.Run(*leftPipe, *outputPipe, keepMe, numAttsIn, numAttsOut);
    }

    void WaitUntilDone() {
        project.WaitUntilDone();
    }

    void Print() {
        cout << "\n ProjectNode : ";
        printPipeIDs();
        printOutputSchema(outschema);
    }
};
class JoinNode : public GenericNode {
   public:
    CNF cnf;
    Record literal;
    Join join;
    Schema *lSchema, *rSchema;

    JoinNode(struct AndList *curr, string rel1, string rel2, unordered_map<string, GenericNode *> &relToNode) {
        GenericNode *lNode = NULL;
        GenericNode *rNode = NULL;

        if (relToNode.count(rel1) != 0 && relToNode.count(rel2) != 0) {
            // both rels already exist
            lNode = relToNode[rel1];
            rNode = relToNode[rel2];
        } else if (relToNode.count(rel1) != 0 && relToNode.count(rel2) == 0) {
            // first exists second does not exist
            lNode = relToNode[rel1];

            Schema *myschema = new Schema("catalog", rel2.c_str());
            DBFile *dbfile = new DBFile();
            string temp_dir = "data/" + rel2 + ".bin";
            const char *dir = temp_dir.c_str();
            dbfile->Open(dir);
            dbfile->MoveFirst();

            rNode = new SelectFileNode(dbfile, myschema, NULL);
            relToNode[rel2] = rNode;

        } else if (relToNode.count(rel1) == 0 && relToNode.count(rel2) != 0) {
            // first does not exist, second does
            rNode = relToNode[rel2];

            Schema *myschema = new Schema("catalog", rel1.c_str());
            DBFile *dbfile = new DBFile();
            string temp_dir = "data/" + rel1 + ".bin";
            const char *dir = temp_dir.c_str();
            dbfile->Open(dir);
            dbfile->MoveFirst();

            lNode = new SelectFileNode(dbfile, myschema, NULL);
            relToNode[rel1] = lNode;

        } else if (relToNode.count(rel1) == 0 && relToNode.count(rel2) == 0) {
            // both do not exist

            // for left node
            Schema *myschema = new Schema("catalog", rel1.c_str());
            DBFile *dbfile = new DBFile();
            string temp_dir = "data/" + rel1 + ".bin";
            const char *dir = temp_dir.c_str();
            dbfile->Open(dir);
            dbfile->MoveFirst();
            lNode = new SelectFileNode(dbfile, myschema, NULL);
            relToNode[rel1] = lNode;

            // for right node
            myschema = new Schema("catalog", rel2.c_str());
            dbfile = new DBFile();
            temp_dir = "data/" + rel2 + ".bin";
            dir = temp_dir.c_str();
            dbfile->Open(dir);
            dbfile->MoveFirst();
            rNode = new SelectFileNode(dbfile, myschema, NULL);
            relToNode[rel2] = rNode;
        }

        lSchema = lNode->outschema;
        rSchema = rNode->outschema;
        this->leftPipe = lNode->outputPipe;
        this->rightPipe = rNode->outputPipe;

        // change mappings for rel1 , rel2 and for a new one called rel1_rel2
        this->lChild = relToNode[rel1];
        this->rChild = relToNode[rel2];
        relToNode[rel1] = this;
        relToNode[rel2] = this;
        string joinTemp = rel1 + "_" + rel2;
        relToNode[joinTemp] = this;

        removeMapping(curr->left);
        cnf.GrowFromParseTree(curr, lSchema, rSchema, literal);

        // create the new outschema which will have atts of both left and right
        int numAtts = lSchema->GetNumAtts() + rSchema->GetNumAtts();
        struct Attribute *mergeAtts = new Attribute[numAtts];
        struct Attribute *atts1 = lSchema->GetAtts();
        struct Attribute *atts2 = rSchema->GetAtts();

        int k = 0;  // index for mergeAtts
        for (int i = 0; i < lSchema->GetNumAtts(); i++) {
            mergeAtts[k].myType = atts1[i].myType;
            mergeAtts[k].name = strdup(atts1[i].name);
            k++;
        }

        for (int i = 0; i < rSchema->GetNumAtts(); i++) {
            mergeAtts[k].myType = atts2[i].myType;
            mergeAtts[k].name = strdup(atts2[i].name);
            k++;
        }

        // now k = size;
        char *join_name = &(rel1 + rel2)[0u];
        outschema = new Schema(join_name, numAtts, mergeAtts);
    }

    void execute() {
        join.Run(*leftPipe, *rightPipe, *outputPipe, cnf, literal);
    }

    void WaitUntilDone() {
        join.WaitUntilDone();
    }

    void Print() {
        cout << "\n Join Node: ";
        printPipeIDs();
        printOutputSchema(outschema);
        cout << "\n Join CNF: ";
        cnf.Print();
    }
};

class DistinctNode : public GenericNode {
   public:
    Schema *inSchema;
    DuplicateRemoval duprem;

    DistinctNode(GenericNode *root, struct NameList *attsToSelect) {
        leftPipe = root->outputPipe;
        outschema = root->outschema;
        struct NameList *head = attsToSelect;

        Attribute *atts = new Attribute[outschema->GetNumAtts()];

        int numatts = 0;
        while (head != NULL) {
            searchAtt(head->name, outschema, (atts + numatts));
            numatts++;
            head = head->next;
        }
        char *filename = "DistinctSchema";
        inSchema = new Schema(filename, numatts, atts);
    }

    void execute() {
        duprem.Run(*leftPipe, *outputPipe, *inSchema);
    }

    void WaitUntilDone() {
        duprem.WaitUntilDone();
    }

    void Print() {
        cout << "\n Distinct Node: ";
        printPipeIDs();
        printOutputSchema(outschema);
    }
};

class GroupByNode : public GenericNode {
   public:
    OrderMaker *om;
    Schema *inSchema;
    Function func;
    GroupBy gb;

    GroupByNode(GenericNode *root, struct NameList *groupingAtts, struct FuncOperator *finalFunction) {
        leftPipe = root->outputPipe;
        cleanFuncOperator(finalFunction);
        func.GrowFromParseTree(finalFunction, *(root->outschema));
        struct NameList *head = attsToSelect;
        //TODO: Figure out OutSchema
        outschema = root->outschema;

        Attribute *atts = new Attribute[outschema->GetNumAtts()];

        int numatts = 0;
        while (head != NULL) {
            searchAtt(head->name, outschema, (atts + numatts));
            numatts++;
            head = head->next;
        }
        char *filename = "GroupBySchema";
        inSchema = new Schema(filename, numatts, atts);
        om = new OrderMaker(inSchema);
    }

    void execute() {
        gb.Run(*leftPipe, *outputPipe, *om, func);
    }

    void WaitUntilDone() {
        gb.WaitUntilDone();
    }

    void Print() {
        cout << "\n GroupBy Node: ";
        printPipeIDs();
        cout << " Order Maker: ";
        om->Print();
        cout << " Function: \n";
        func.Print();
    }
};

// does the Function computation
class FunctionNode : public GenericNode {
   public:
    Function func;
    Sum sum;

    FunctionNode(GenericNode *root, FuncOperator *finalFunction) {
        leftPipe = root->outputPipe;
        func.GrowFromParseTree(finalFunction, *(root->outschema));
    }

    void execute() {
        sum.Run(*leftPipe, *outputPipe, func);
    }

    void WaitUntilDone() {
        sum.WaitUntilDone();
    }

    void Print() {
        cout << "\n Function Node: ";
        printPipeIDs();
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
    void inOrderPrint(GenericNode *currentNode);
    void assignPipeIDs(GenericNode *root);
    // void printPipeIDs(GenericNode *node);
    void cleanup();

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

    GenericNode *node;

    if (numToJoin == 1) {
        // it is a selection
        if (relToNode.count(rel1) != 0) {
            // make a selectPipe node
            // get schema from the outpipe of whatever executed earlier of that relation
            Schema *inSchema = relToNode[rel1]->outschema;
            // now this will become schema for the new Node
            node = new SelectPipeNode(relToNode[rel1]->outputPipe, inSchema, curr, relToNode[rel1]);

        } else {
            // make a selectFile node
            // new - get schema from Catalog
            Schema *myschema = new Schema("catalog", rel1.c_str());
            DBFile *dbfile = new DBFile();
            string temp_dir = "data/" + rel1 + ".bin";
            const char *dir = temp_dir.c_str();
            dbfile->Open(dir);
            node = new SelectFileNode(dbfile, myschema, curr);
        }

        // here add to the map
        relToNode[rel1] = node;
    } else if (numToJoin == 2) {
        // then it is a join
        // get the left relation and attr name
        temp = string(curr->left->left->right->value);
        string rel2 = temp.substr(0, temp.find("."));
        string rel2Attr = temp.substr(temp.find(".") + 1, temp.size());

        if (aliasToRel.count(rel2)) {
            rel2 = aliasToRel[rel2];
        }

        node = new JoinNode(curr, rel1, rel2, relToNode);
    }

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

        double currEst = stats->Estimate(currAndList, NULL, 0);
        estimates.push_back(currEst);
        andList = andList->rightAnd;
    }

    // now select the AndList of the min Cost
    int minIndex = 0;
    double temp = estimates[0];
    for (int i = 0; i < estimates.size(); i++) {
        if (estimates[i] < temp) {
            minIndex = i;
            temp = estimates[i];
        }
    }

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

    if (target == candidates) {  // the node to be removed is the first node.
        candidates = target->rightAnd;
        target->rightAnd = NULL;
    } else {  // the node to be removed is in the middle or tail.
        pre->rightAnd = target->rightAnd;
        target->rightAnd = NULL;
    }

    currAndList->left = target->left;
    stats->Apply(target, NULL, 1);

    struct OrList *left = target->left;
    struct ComparisonOp *compLeft = left->left;
    if (compLeft->left->code == NAME && compLeft->right->code == NAME) {
        numToJoin = 2;
    } else {
        // it is a selection, either the first or the second is a attsName
        numToJoin = 1;
    }

    // now make a treeNode out of the current AndList
    // set that as the root node
    // the last one to be set will be the largest costing AndList
    root = CreateTreeNode(target, numToJoin);

    // recurse to get the next minimum AndList
    AndListEval(candidates);
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

    // check for non aggr distinct
    if (distinctAtts == 1) {
        // cout << "\n Stage: Distinct non aggr";
        DistinctNode *dn = new DistinctNode(root, attsToSelect);
        dn->lChild = root;
        root = dn;
    }

    if (distinctFunc == 1) {
        // cout << "\n Stage: Distinct aggr";
        struct NameList *atts, *head;
        getAttsFromFunc(finalFunction, atts, head);
        DistinctNode *dn = new DistinctNode(root, head);
        dn->lChild = root;
        root = dn;

        FunctionNode *fn = new FunctionNode(root, finalFunction);
        fn->lChild = root;
        root = fn;
        // cout << "\n Distinct func set";
    } else if (finalFunction != NULL && groupingAtts == NULL) {
        // cout << "\n Stage: Function";
        cleanFuncOperator(finalFunction);
        FunctionNode *fn = new FunctionNode(root, finalFunction);
        fn->lChild = root;
        root = fn;
    } else if (finalFunction != NULL && groupingAtts != NULL) {
        // cout << "\n Stage: Func + GroupBy";
        GroupByNode *gbn = new GroupByNode(root, groupingAtts, finalFunction);
        gbn->lChild = root;
        root = gbn;
    }

    // for projections
    if (attsToSelect != NULL) {
        // cout << "\n Project Node: ";
        ProjectNode *pn = new ProjectNode(attsToSelect, root);
        pn->lChild = root;
        root = pn;
    }

    assignPipeIDs(root);

    cout << "\n Printing the current tree(In-Order): ";
    inOrderPrint(root);

    cleanup();
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
}

void Query::inOrderPrint(GenericNode *currentNode) {
    if (!currentNode) {
        return;
    }
    inOrderPrint(currentNode->lChild);
    currentNode->Print();
    inOrderPrint(currentNode->rChild);
}

void Query::assignPipeIDs(GenericNode *node) {
    if (!node) {
        return;
    }
    assignPipeIDs(node->lChild);
    assignPipeIDs(node->rChild);

    if (node->outputPipe != NULL) {
        if (outputpipemap.count(node->outputPipe) == 0) {
            outputpipemap[node->outputPipe] = outp++;
        }
    }

    if (node->leftPipe != NULL) {
        if (inputpipemap.count(node->leftPipe) == 0) {
            inputpipemap[node->leftPipe] == inp++;
        }
    }

    if (node->rightPipe != NULL) {
        if (inputpipemap.count(node->rightPipe) == 0) {
            inputpipemap[node->rightPipe] == inp++;
        }
    }
}

void GenericNode::printPipeIDs() {
    if (leftPipe != NULL) {
        cout << "\n Left Input Pipe ID: " << outputpipemap[leftPipe];
    }

    if (rightPipe != NULL) {
        cout << "\n Right Input Pipe ID: " << outputpipemap[rightPipe];
    }

    if (outputPipe != NULL) {
        cout << "\n Output Pipe ID: " << outputpipemap[outputPipe];
    }

    cout << "\n";
}

void Query::cleanup() {
    inputpipemap.clear();
    outputpipemap.clear();
    inp = 0;
    outp = 0;

    finalFunction = NULL;
    tables = NULL;
    boolean = NULL;
    groupingAtts = NULL;
    attsToSelect = NULL;
    distinctAtts = 0;
    distinctFunc = 0;
}