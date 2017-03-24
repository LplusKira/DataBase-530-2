
#ifndef PARSER_TYPES_H
#define PARSER_TYPES_H

#include <iostream>
#include <stdlib.h>
#include "ExprTree.h"
#include "MyDB_Catalog.h"
#include "MyDB_Schema.h"
#include "MyDB_Table.h"
#include <string>
#include <utility>
#include <stdio.h>
#include <string.h>

using namespace std;

/*************************************************/
/** HERE WE DEFINE ALL OF THE STRUCTS THAT ARE **/
/** PASSED AROUND BY THE PARSER                **/
/*************************************************/

// structure that encapsulates a parsed computation that returns a value
struct Value {

private:

        // this points to the expression tree that computes this value
        ExprTreePtr myVal;

public:
        ~Value () {}

        Value (ExprTreePtr useMe) {
            myVal = useMe;
        }
        Value () {
            myVal = nullptr;
        }
	
	friend struct CNF;
	friend struct ValueList;
	friend struct SFWQuery;
	#include "FriendDecls.h"
};

// structure that encapsulates a parsed CNF computation
struct CNF {

private:

        // this points to the expression tree that computes this value
        vector <ExprTreePtr> disjunctions;

public:
        ~CNF () {}

        CNF (struct Value *useMe) {
              	disjunctions.push_back (useMe->myVal); 
        }

        CNF () {}

	friend struct SFWQuery;
	#include "FriendDecls.h"
};

// structure that encapsulates a parsed list of value computations
struct ValueList {

private:

        // this points to the expression tree that computes this value
        vector <ExprTreePtr> valuesToCompute;

public:
        ~ValueList () {}

        ValueList (struct Value *useMe) {
              	valuesToCompute.push_back (useMe->myVal); 
        }
    vector <ExprTreePtr> getValues () {
        return valuesToCompute;
    }
        ValueList () {}

	friend struct SFWQuery;
	#include "FriendDecls.h"
};


// structure to encapsulate a create table
struct CreateTable {

private:

	// the name of the table to create
	string tableName;

	// the list of atts to create... the string is the att name
	vector <pair <string, MyDB_AttTypePtr>> attsToCreate;

	// true if we create a B+-Tree
	bool isBPlusTree;

	// the attribute to organize the B+-Tree on
	string sortAtt;

public:
	string addToCatalog (string storageDir, MyDB_CatalogPtr addToMe) {

		// make the schema
		MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema>();
		for (auto a : attsToCreate) {
			mySchema->appendAtt (a);
		}

		// now, make the table
		MyDB_TablePtr myTable;

		// just a regular file
		if (!isBPlusTree) {
			myTable =  make_shared <MyDB_Table> (tableName, 
				storageDir + "/" + tableName + ".bin", mySchema);	

		// creating a B+-Tree
		} else {
			myTable =  make_shared <MyDB_Table> (tableName, 
				storageDir + "/" + tableName + ".bin", mySchema, "bplustree", sortAtt);	
		}

		// and add to the catalog
		myTable->putInCatalog (addToMe);

		return tableName;
	}

	CreateTable () {}

	CreateTable (string tableNameIn, vector <pair <string, MyDB_AttTypePtr>> atts) {
		tableName = tableNameIn;
		attsToCreate = atts;
		isBPlusTree = false;
	}

	CreateTable (string tableNameIn, vector <pair <string, MyDB_AttTypePtr>> atts, string sortAttIn) {
		tableName = tableNameIn;
		attsToCreate = atts;
		isBPlusTree = true;
		sortAtt = sortAttIn;
	}
	
	~CreateTable () {}

	#include "FriendDecls.h"
};

// structure that stores a list of attributes
struct AttList {

private:

	// the list of attributes
	vector <pair <string, MyDB_AttTypePtr>> atts;

public:
	AttList (string attName, MyDB_AttTypePtr whichType) {
		atts.push_back (make_pair (attName, whichType));
	}

	~AttList () {}

	friend struct SFWQuery;
	#include "FriendDecls.h"
};

struct FromList {

private:

	// the list of tables and aliases
	vector <pair <string, string>> aliases;

public:
	FromList (string tableName, string aliasName) {
		aliases.push_back (make_pair (tableName, aliasName));
	}

	FromList () {}

	~FromList () {}
	
	friend struct SFWQuery;
	#include "FriendDecls.h"
};


// structure that stores an entire SFW query
struct SFWQuery {

private:

	// the various parts of the SQL query
	vector <ExprTreePtr> valuesToSelect;
	vector <pair <string, string>> tablesToProcess;
	vector <ExprTreePtr> allDisjunctions;
	vector <ExprTreePtr> groupingClauses;

public:
	SFWQuery () {}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf, struct ValueList *grouping) {
		valuesToSelect = selectClause->valuesToCompute;
		tablesToProcess = fromClause->aliases;
		allDisjunctions = cnf->disjunctions;
		groupingClauses = grouping->valuesToCompute;
	}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf) {
		valuesToSelect = selectClause->valuesToCompute;
		tablesToProcess = fromClause->aliases;
		allDisjunctions = cnf->disjunctions;
	}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
		valuesToSelect = selectClause->valuesToCompute;
		tablesToProcess = fromClause->aliases;
		allDisjunctions.push_back (make_shared <BoolLiteral> (true));
	}
	
	~SFWQuery () {}

	void print () {
		cout << "Selecting the following:\n";
		for (auto a : valuesToSelect) {
			cout << "\t" << a->toString () << "\n";
		}
		cout << "From the following:\n";
		for (auto a : tablesToProcess) {
			cout << "\t" << a.first << " AS " << a.second << "\n";
            
		}
		cout << "Where the following are true:\n";
		for (auto a : allDisjunctions) {
			cout << "\t" << a->toString () << "\n";
		}
		cout << "Group using:\n";
		for (auto a : groupingClauses) {
			cout << "\t" << a->toString () << "\n";
		}
	}
//    vector<string> split(const string &s, const string &seperator){
//        vector<string> result;
//        typedef string::size_type string_size;
//        string_size i = 0;
//        
//        while(i != s.size()){
//            //找到字符串中首个不等于分隔符的字母；
//            int flag = 0;
//            while(i != s.size() && flag == 0){
//                flag = 1;
//                for(string_size x = 0; x < seperator.size(); ++x)
//                    if(s[i] == seperator[x]){
//                        　　++i;
//                        　　flag = 0;
//                        　　 break;
//                        　　}
//            }
//            
//            //找到又一个分隔符，将两个分隔符之间的字符串取出；
//            flag = 0;
//            string_size j = i;
//            while(j != s.size() && flag == 0){
//                for(string_size x = 0; x < seperator.size(); ++x)
//                    　　if(s[j] == seperator[x]){
//                        　　flag = 1;
//                        　　 break;
//                        　　}
//                if(flag == 0) 
//                    　　++j;
//            }
//            if(i != j){
//                result.push_back(s.substr(i, j-i));
//                i = j;
//            }
//        }
//        return result;
//    }
//    void checkAtt(MyDB_CatalogPtr catalog, ExprTreePtr a) {
//        // check if att is in there
//        cout << "This is an att\n";
//        vector<string> v = split(a->toString(), "[]@");
//        string tableName_short = v[0];
//        string attName = v[1];
//                
//        string tableName;
//        for (auto a: tablesToProcess) {
//            if (a.second == tableName_short)
//                tableName = a.first;
//        }
//                
//        MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema> ();
//        mySchema->fromCatalog (tableName, catalog);
//        vector <pair <string, MyDB_AttTypePtr>> atts = mySchema->getAtts();
//        bool attisthere = false;
//        for (pair <string, MyDB_AttTypePtr> att: atts) {
//            if (att.first == attName) {
//                attisthere = true;
//                cout << att.first << ", " << att.second->toString() << ".\n";
//                break;
//            }
//        }
//        if (!attisthere) {
//            cout << "\n\nError: There is no att named ("<< attName << ") in table [" << tableName <<"].\n\n";
//        }
//    }
    void check(MyDB_CatalogPtr catalog) {
        bool pass = true;
        vector <string> myTables;
        catalog->getStringList ("tables", myTables);
        
        // check if table is in there
        for (auto a : tablesToProcess) {
            bool tbinthere = false;
            for (string s : myTables) {
                //cout << "database has table: " << s << ",";
                if (s == a.first) {
                    tbinthere = true;
                    break;
                }
            }
            if (!tbinthere) {
                cout << "ERROR: There is no table named ["<< a.first << "] in this database.\n";
                pass = false;
                return;
            }
        }
        vector<ExprTreePtr> groupAtts;
        bool shouldGroup = false;
        for (auto a: valuesToSelect) {
            if (a->type() == "Identifier") {
                groupAtts.push_back(a);
            } else {
                shouldGroup = true;
            }
            string result = a->check(catalog, tablesToProcess);
            if (result == "ERROR") {
                pass = false;
                return;
            }
        }
        
        for (auto a: allDisjunctions) {
            string result = a->check(catalog, tablesToProcess);
            if (result == "ERROR") {
                pass = false;
                return;
            }
        }
        
        if (!shouldGroup) {
            
        } else {
            for (ExprTreePtr b: groupAtts) {
                cout << "group atts: " << b->toString() << "\n";
                bool hasB = false;
                for (auto a: groupingClauses) {
                    cout << "group clauses: " << a->toString() << "\n";
                    if (a->toString() == b->toString()) {
                        hasB = true;
                    }
                }
                if (!hasB) {
                    cout << "ERROR: selecting on " << b->toString() << "but it is not in the group by clause or an aggregate.\n";
                    pass = false;
                    return;
                }
            }
        }
        if (pass) {
            cout << "Query OK!\n";
        }
        
    }
	#include "FriendDecls.h"
};

// structure that sores an entire SQL statement
struct SQLStatement {

private:

	// in case we are a SFW query
	SFWQuery myQuery;
	bool isQuery;

	// in case we a re a create table
	CreateTable myTableToCreate;
	bool isCreate;

public:
    
	SQLStatement (struct SFWQuery* useMe) {
		myQuery = *useMe;
		isQuery = true;
		isCreate = false;
	}

	SQLStatement (struct CreateTable *useMe) {
		myTableToCreate = *useMe;
		isQuery = false;
		isCreate = true;
	}

	bool isCreateTable () {
		return isCreate;
	}

	bool isSFWQuery () {
		return isQuery;
	}

	string addToCatalog (string storageDir, MyDB_CatalogPtr addToMe) {
		return myTableToCreate.addToCatalog (storageDir, addToMe);
	}		
	
	void printSFWQuery () {
		myQuery.print ();
	}
    void checkSFWQuery(MyDB_CatalogPtr checkInMe) {
        myQuery.check(checkInMe);
    }
	#include "FriendDecls.h"
};

#endif
