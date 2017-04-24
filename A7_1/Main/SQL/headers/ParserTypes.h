
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
#include "MyDB_BufferManager.h"
#include "MyDB_TableReaderWriter.h"
#include "RegularSelection.h"
#include "Aggregate.h"
#include "MyDB_AttType.h"

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
			
			// make sure that we have the attribute
			if (mySchema->getAttByName (sortAtt).first == -1) {
				cout << "B+-Tree not created.\n";
				return "nothing";
			}
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
    void getRA(MyDB_CatalogPtr myCatalog, MyDB_BufferManagerPtr myMgr, map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters) {
        cout << "All tables in current catalog:\n";
        map <string, MyDB_TablePtr> allTables = MyDB_Table::getAllTables(myCatalog);
        for (std::map<string, MyDB_TablePtr>::iterator it=allTables.begin(); it!=allTables.end(); ++it)
            std::cout << it->first << " => " << *it->second << "\n";
        
        cout << "From the following table:\n";
        for (auto a : tablesToProcess) {
            string tableName = a.second;
            cout << "\t" << a.first << " AS " << a.second << "\n";
            cout << "\t create TableReaderWriter...\n" << flush;
            
            MyDB_TableReaderWriterPtr supplierTable = allTableReaderWriters[a.first];

            //create output schema
            MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
            vector <string> projections;
            vector <pair <MyDB_AggType, string>> aggsToCompute;
            vector <string> groupings;
            string selectionPredicate = "";
            cout << "Selecting the following:\n";
            for (auto a : valuesToSelect) {
                cout << "\t" << a->toString () << "\n";
                if (a->getType() == "regular") {
                    projections.push_back(a->toString());
                    vector<pair<string, string>> atts = a->getAttsTables();
                    for (pair<string, string> att: atts) {
                        cout << "atts:" << att.first << " in " << att.second << "\n";
                        if (att.second == tableName) {
                            cout << "schema append: " << att.first << ", type :" << supplierTable->getTable()->getSchema()->getAttByName(att.first).second->toString() << "\n";
                            mySchemaOut->appendAtt(make_pair (att.first, supplierTable->getTable()->getSchema()->getAttByName(att.first).second));
                        }
                    }
                } else {
                    if (a->getType() == "sum") {
                        cout << "sum this: " <<  a->getChild()->toString() << "\n";
                        if(a->getChild()->toString() == "int[1]"){
                            aggsToCompute.push_back (make_pair (MyDB_AggType :: cnts, "int[0]"));
                            int number = rand() % 100;
                            string name = "cnt" + std::to_string(number);
                            mySchemaOut->appendAtt(make_pair (name, make_shared <MyDB_IntAttType>()));
                        }else{
                            aggsToCompute.push_back (make_pair (MyDB_AggType :: sums, a->getChild()->toString()));
                            int number = rand() % 100;
                            string name = "sum" + std::to_string(number);
                            mySchemaOut->appendAtt(make_pair (name, make_shared <MyDB_DoubleAttType>()));
                        }
                    } else if (a->getType() == "avg") {
                        cout << "avg this: " <<  a->getChild()->toString() << "\n";
                        aggsToCompute.push_back (make_pair (MyDB_AggType :: avgs, a->getChild()->toString()));
                         int number = rand() % 100;
                        string name = "avg" + std::to_string(number);
                        mySchemaOut->appendAtt(make_pair (name, make_shared <MyDB_DoubleAttType>()));
                    }
                }
                
            }
            cout << "Where the following are true:\n";
            string firstPredicate;
            string secondPredicate;
            if (allDisjunctions.size() > 0) {
                firstPredicate = allDisjunctions[0]->toString();
            }
            
            for (int i = 1; i < allDisjunctions.size(); i++ ) {
                secondPredicate = allDisjunctions[i]->toString();
                selectionPredicate = "&& ( " + firstPredicate + "," + secondPredicate + ")";
                cout << "check pred: " << selectionPredicate << "\n";
                firstPredicate = selectionPredicate;
            }
            
            for (auto a : groupingClauses) {
                groupings.push_back(a->toString());
            }

            cout << "selectionPredicate: " << selectionPredicate << "\n";
            MyDB_TablePtr myTableOut = make_shared <MyDB_Table> (a.first + "Out", a.first + "Out.bin", mySchemaOut);
            MyDB_TableReaderWriterPtr supplierTableOut = make_shared <MyDB_TableReaderWriter> (myTableOut, myMgr);
            
            // do aggregate or regular selection
            if (aggsToCompute.size() != 0){
                Aggregate myOp(supplierTable, supplierTableOut, aggsToCompute, groupings, selectionPredicate);
                myOp.run();
            }else{
                RegularSelection myOp (supplierTable, supplierTableOut, selectionPredicate, projections);
                myOp.run ();
            }
            MyDB_RecordPtr temp = supplierTableOut->getEmptyRecord ();
            MyDB_RecordIteratorAltPtr myIter = supplierTableOut->getIteratorAlt ();
            int count  = 0;
            while (myIter->advance ()) {
                myIter->getCurrent (temp);
                cout << temp << "\n";
                count++;
            }
            cout << "count : " << count << "\n";
            
        }

        
        
    }
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
	
	void printSFWQuery (MyDB_CatalogPtr myCatalog, MyDB_BufferManagerPtr myMgr, map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters) {
        myQuery.getRA(myCatalog, myMgr, allTableReaderWriters);
		//myQuery.print ();
	}

	#include "FriendDecls.h"
};

#endif
