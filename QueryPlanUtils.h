#include <iostream>
#include "Function.h"
#include "ParseFunc.h"
#include "ParseTree.h"

using namespace std;

void printFuncOperator(struct FuncOperator *finalFunction) {
    cout << "\n Code: " << finalFunction->code;

    if (finalFunction->leftOperand != NULL) {
        cout << "\n Left Operand Code: " << (finalFunction->leftOperand->code);
        cout << "\n Left Operand Value: " << string(finalFunction->leftOperand->value);
    }

    if (finalFunction->leftOperator != NULL) {
        cout << "\n Left Operator: ";
        printFuncOperator(finalFunction->leftOperator);
    }

    if (finalFunction->right != NULL) {
        cout << "\n Right Operator: ";
        printFuncOperator(finalFunction->right);
    }

    cout << endl;
}

void printTableList(struct TableList *tables) {
    while (tables != NULL) {
        cout << "\n Table Name: " << string(tables->tableName);
        cout << "\n Table Alias: " << string(tables->aliasAs);
        tables = tables->next;
    }
    cout << endl;
}

void printNameList(struct NameList *nl) {
    while (nl != NULL) {
        cout << "\n Name: " << string(nl->name);
        nl = nl->next;
    }
    cout << endl;
}

void printOrList(struct OrList *ol) {
    while (ol != NULL) {
        cout << "\n Comparision Op Code: " << ol->left->code;
        cout << "\n Comparision Operand Left: " << ol->left->left->code;
        cout << "\n Comparision Operand Left: " << string(ol->left->left->value);
        cout << "\n Comparision Operand Right: " << ol->left->right->code;
        cout << "\n Comparision Operand Right: " << string(ol->left->right->value);
        ol = ol->rightOr;
    }
}

void printAndList(struct AndList *al) {
    while (al != NULL) {
        printOrList(al->left);
        al = al->rightAnd;
    }
    cout << endl;
}

char *removeDot(char *str) {
    string temp = string(str);
    string attName;
    if (temp.find(".") != string::npos) {
        attName = temp.substr(temp.find(".") + 1, string::npos);
        str = new char[attName.length() + 1];
        strcpy(str, attName.c_str());
        cout << "\n AttName new (without the .): " << string(str);
    }
    return str;
}

void removeDot(struct Operand *op) {
    string temp = string(op->value);
    string attName;
    if (temp.find(".") != string::npos) {
        attName = temp.substr(temp.find(".") + 1, string::npos);
        delete[] op->value;
        op->value = new char[attName.length() + 1];
        strcpy(op->value, attName.c_str());
        cout << "\n AttName new (without the .): " << string(op->value);
    }
}

void cleanFuncOperator(struct FuncOperator *finalFunction) {

    if (finalFunction->leftOperand != NULL) {
        finalFunction->leftOperand->value = removeDot(finalFunction->leftOperand->value);
    }

    if (finalFunction->leftOperator != NULL) {
        cleanFuncOperator(finalFunction->leftOperator);
    }

    if (finalFunction->right != NULL) {
        cleanFuncOperator(finalFunction->right);
    }

}

void searchAtt(char *attname, Schema *s, Attribute *copyToMe) {
    cout << "\n Distinct Stage 5";
    cout << "\n Attname: " << string(attname);
    // remove the dot before searching
    attname = removeDot(attname);
    cout << "\n After dot removal attname: " << string(attname);

    // search for the current attname in the schema
    Attribute *atts = s->GetAtts();
    for (int i = 0; i < s->GetNumAtts(); i++) {
        // cout << "\n Names in Loop: " << string((atts + i)->name);
        // cout << "\n Types in Loop: " << (atts + i)->myType;

        if (strcmp(attname, (atts + i)->name) == 0) {
            copyToMe->myType = (atts + i)->myType;
            copyToMe->name = strdup((atts + i)->name);

            cout << "\n Name: " << string(copyToMe->name);
            cout << "\n Type: " << copyToMe->myType;
            return;
        }
    }
    return;
}

// remove the ____._____ from the AndList
void removeMapping(struct OrList *orlist) {
    cout << "\n In remove Mapping...";
    string attName;
    while (orlist != NULL) {
        if (orlist->left->left->code == 3) {
            removeDot(orlist->left->left);
        }
        if (orlist->left->right->code == 3) {
            removeDot(orlist->left->right);
        }
        orlist = orlist->rightOr;
    }
}

/*******************************************************************************
 * Helper function to print output schema of each node
 ******************************************************************************/
void printOutputSchema(Schema *s) {
    cout << "Output Schema: " << endl;
    for (int i = 0; i < s->GetNumAtts(); i++) {
        Attribute *att = s->GetAtts();
        cout << "    " << att[i].name << ": ";
        // cout << "    Att" << (i+1) << ": ";
        switch (att[i].myType) {
            case 0:
                cout << "Int" << endl;
                break;
            case 1:
                cout << "Double" << endl;
                break;
            case 2:
                cout << "String" << endl;
                break;
        }
    }
}