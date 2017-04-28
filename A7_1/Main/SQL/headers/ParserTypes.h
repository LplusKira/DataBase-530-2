
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
#include "SortMergeJoin.h"
#include "MyDB_AttType.h"
#include "ScanJoin.h"
#include <unordered_map>
#include <stack>
#include <time.h>
#include <stdio.h>

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
//				cout << "B+-Tree not created.\n";
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
    vector <string> deleteMe;

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
    
    void oneTableCase(MyDB_CatalogPtr myCatalog, MyDB_BufferManagerPtr myMgr, map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, string storageDir) {
        int t = clock();
        MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
        vector <string> projections;
        vector <pair <MyDB_AggType, string>> aggsToCompute;
        vector <string> groupings;
        string selectionPredicate = "";
        MyDB_TableReaderWriterPtr supplierTable;
        auto a = tablesToProcess.at(0);
        string tableName = a.second;
        bool groupFirst = true;
//        cout << "\t" << a.first << " AS " << a.second << "\n";
//        cout << "\t create TableReaderWriter...\n" << flush;
        
        supplierTable = allTableReaderWriters[a.first];
        
        //create output schema
//        cout << "Selecting the following:\n";
        vector <ExprTreePtr> aggs;
        if (valuesToSelect.at(0)->getType() != "regular") {
            groupFirst = false;
        }
        for (auto selected : valuesToSelect) {
//            cout << "\t" << selected->toString () << "\n";
            if (selected->getType() == "regular") {
//                cout << "a type is regular" << selected->toString() << "\n";
                projections.push_back(selected->toString());
                groupings.push_back(selected->toString());
                vector<pair<string, string>> atts = selected->getAttsTables();
                for (pair<string, string> att: atts) {
//                    cout << "atts:" << att.first << " in " << att.second << "\n";
                    if (att.second == tableName) {
//                        cout << "schema append: " << att.first << ", type :" << supplierTable->getTable()->getSchema()->getAttByName(att.first).second->toString() << "\n";
                        mySchemaOut->appendAtt(make_pair (att.first, supplierTable->getTable()->getSchema()->getAttByName(att.first).second));
                    }
                }
            } else {
                if (selected->getType() == "sum") {
//                    cout << "sum this: " <<  selected->getChild()->toString() << "\n";
                    if(selected->getChild()->toString() == "int[1]"){
                        aggsToCompute.push_back (make_pair (MyDB_AggType :: cnts, "int[0]"));
                        int number = rand() % 100;
                        string name = "cnt" + std::to_string(number);
                        mySchemaOut->appendAtt(make_pair (name, make_shared <MyDB_IntAttType>()));
                    }else{
                        aggsToCompute.push_back (make_pair (MyDB_AggType :: sums, selected->getChild()->toString()));
                        int number = rand() % 100;
                        string name = "sum" + std::to_string(number);
                        mySchemaOut->appendAtt(make_pair (name, make_shared <MyDB_DoubleAttType>()));
                    }
                } else if (selected->getType() == "avg") {
//                    cout << "avg this: " <<  selected->getChild()->toString() << "\n";
                    aggsToCompute.push_back (make_pair (MyDB_AggType :: avgs, selected->getChild()->toString()));
                    int number = rand() % 100;
                    string name = "avg" + std::to_string(number);
                    mySchemaOut->appendAtt(make_pair (name, make_shared <MyDB_DoubleAttType>()));
                }
            }
            
        }
        
//        cout << "Where the following are true:\n";
        string firstPredicate;
        string secondPredicate;
        if (allDisjunctions.size() > 0) {
            firstPredicate = allDisjunctions[0]->toString();
            
        }
        
        for (int i = 1; i < allDisjunctions.size(); i++ ) {
            secondPredicate = allDisjunctions[i]->toString();
            selectionPredicate = "&& ( " + firstPredicate + "," + secondPredicate + ")";
//            cout << "check pred: " << selectionPredicate << "\n";
            firstPredicate = selectionPredicate;
        }
        
        if (allDisjunctions.size() == 1) {
            selectionPredicate = firstPredicate;
        }
        
//        cout << "selectionPredicate: " << selectionPredicate << "\n";
        int outtableNumber = rand() % 100;
        string outTableName = "Join" + std::to_string(outtableNumber) + "Out";
        MyDB_TablePtr myTableOut = make_shared <MyDB_Table> (outTableName, storageDir + "/" + outTableName + ".bin", mySchemaOut);
//        cout << "deleteMe push: " << myTableOut->getStorageLoc() << "\n";
        deleteMe.push_back(myTableOut->getStorageLoc());
        MyDB_TableReaderWriterPtr supplierTableOut = make_shared <MyDB_TableReaderWriter> (myTableOut, myMgr);
        
        // do aggregate or regular selection
        if (aggsToCompute.size() != 0){
            Aggregate myOp(supplierTable, supplierTableOut, aggsToCompute, groupings, selectionPredicate);
            myOp.run(groupFirst);
        }else{
            RegularSelection myOp (supplierTable, supplierTableOut, selectionPredicate, projections);
            myOp.run ();
        }
        MyDB_RecordPtr temp = supplierTableOut->getEmptyRecord ();
        MyDB_RecordIteratorAltPtr myIter = supplierTableOut->getIteratorAlt ();
        int count  = 0;
        while (myIter->advance ()) {
            myIter->getCurrent (temp);
            if (count < 30){
                 cout << temp << "\n";
            }
            count++;
        }
        t = clock() - t;
        float runningtime = (float)t / CLOCKS_PER_SEC;
        cout << "Total " << count << " rows in set.\n" << "using " << runningtime << " seconds\n";
        
    }
    
    map <string, MyDB_TableReaderWriterPtr> initiateTRWafterFilter(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, MyDB_BufferManagerPtr myMgr, string storageDir) {
        //initiate TRWafterFilter(shortName, tableReaderWriter) with all table's initial TRWs
        map <string, MyDB_TableReaderWriterPtr> TRWafterFilter;
        unordered_map <string, int> myHash;
        for (auto t : tablesToProcess){
            string longName = t.first;
            if(myHash.count(longName) == 0){
                myHash[longName] = 1;
            }else{
                myHash[longName] += 1;
            }
        }
        for (auto t : tablesToProcess){
            string shortName = t.second;
            string longName = t.first;
//            cout << "shortName" << shortName <<"\n";
            MyDB_TableReaderWriterPtr initialTRW;
            if (myHash[longName] == 1 ){
                initialTRW = allTableReaderWriters[longName];
            }else{
                MyDB_TableReaderWriterPtr arw = allTableReaderWriters[longName];
                 MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();
                for (auto &p: arw->getTable()->getSchema()->getAtts()){
                    mySchemaOut->appendAtt(p);
                }
                MyDB_TablePtr copyTable = make_shared<MyDB_Table>(shortName, storageDir+"/"+shortName +".bin", mySchemaOut);
//                cout << "deleteMe push: " << copyTable->getStorageLoc() << "\n";
                deleteMe.push_back(copyTable->getStorageLoc());
                
                initialTRW = make_shared<MyDB_TableReaderWriter>(copyTable, myMgr);
                MyDB_RecordPtr temp = arw->getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = arw->getIteratorAlt ();
                while (myIter->advance ()) {
                    myIter->getCurrent (temp);
                    initialTRW->append(temp);
                }
            }
            
            for (auto &p: initialTRW->getTable()->getSchema()->getAtts()){
//                cout << "name "<<p.first << "\n";
                string name = p.first;
                string delimiter = "_";
                string token = name.substr(0, name.find(delimiter));
                name.replace(0,token.length(),shortName);
                p.first = name;
            }
//            for (auto &p: initialTRW->getTable()->getSchema()->getAtts()){
//                cout <<"schema change short name " << p.first << "\n";
//            }
            TRWafterFilter [shortName] = initialTRW;
        }
        return TRWafterFilter;
    }
    
    map <string, MyDB_TableReaderWriterPtr> updateTRWafterFilter(map <string, MyDB_TableReaderWriterPtr> TRWafterFilter,ExprTreePtr disjunction, MyDB_BufferManagerPtr myMgr, string storageDir) {
//        cout << "REGULAR....\n";
        // if it is a regular selection statement, do a regular selection and update TRWafterFilter
        string selectionPredicate = disjunction->toString();
        vector<string> projections;
        string tbShort = *disjunction->getTables().begin();
        MyDB_TableReaderWriterPtr tbRW = TRWafterFilter [tbShort];
        string tbLong = tbRW->getTable()->getName();
        
        MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();
        for (auto &p: tbRW->getTable()->getSchema()->getAtts()){
            mySchemaOut->appendAtt(p);
            projections.push_back("["+p.first+"]");
        }
        
        int outtableNumber = rand() % 100;
        string outTableName = tbLong + std::to_string(outtableNumber) + "out";
        MyDB_TablePtr myTableOut = make_shared <MyDB_Table> (outTableName, storageDir + "/" + outTableName + ".bin", mySchemaOut);
//        cout << "deleteMe push: " << myTableOut->getStorageLoc() << "\n";
        deleteMe.push_back(myTableOut->getStorageLoc());
        MyDB_TableReaderWriterPtr tbRWOut = make_shared <MyDB_TableReaderWriter> (myTableOut, myMgr);
        RegularSelection myOp (tbRW, tbRWOut, selectionPredicate, projections);
        myOp.run ();
        MyDB_RecordPtr temp = tbRWOut->getEmptyRecord ();
        MyDB_RecordIteratorAltPtr myIter = tbRWOut->getIteratorAlt ();
//        int count  = 0;
//        while (myIter->advance ()) {
//            myIter->getCurrent (temp);
//            //cout << temp << "\n";
//            count++;
//        }
//        cout << "Total " << count << " rows in " << tbRWOut->getTable()->getName()<< ".\n";
        TRWafterFilter [tbShort] = tbRWOut;
        return TRWafterFilter;
    }
    
    pair<vector<pair<string, string>>, string> dealWithJoinPredicate(int i, vector<pair<string, string>> equalityChecks, vector<string> leftShorts, string rightShort) {
//        cout << "\n\nSHOULD JOIN\n";
        string leftOne = "";
        string rightOne = "";
        string finalPredicate = "";
        string leftDisjunctionTB = "";
        string rightDisjunctionTB = "";
        if (allDisjunctions[i]->getLeft() == nullptr || allDisjunctions[i]->getRight() == nullptr) {
//            cout << "NOT a Join\n";
            bool isThatTwo1 = false;
            bool isThatTwo2 = false;
            for (string tbShort: allDisjunctions[i]->getTables()) {
                if (tbShort == rightShort) {
                    isThatTwo1 = true;
                }
                for (string l: leftShorts) {
                    if (tbShort == l) {
                        isThatTwo2 = true;
                        break;
                    }
                }
            }
            if (isThatTwo1 && isThatTwo2) {
                finalPredicate = allDisjunctions[i]->toString();
            } else {
                finalPredicate = "";
            }
            return make_pair(equalityChecks, finalPredicate);
        } else {
            leftDisjunctionTB = *allDisjunctions[i]->getLeft()->getTables().begin();
            rightDisjunctionTB = *allDisjunctions[i]->getRight()->getTables().begin();
        }
        for (string s: leftShorts) {
            if(leftDisjunctionTB == s ){
                leftOne = allDisjunctions[i]->getLeft()->toString();
//                cout  << "leftOne: " << leftOne << "\n";
            }
        }
        if (leftDisjunctionTB == rightShort){
            rightOne = allDisjunctions[i]->getLeft()->toString();
//            cout << "rightOne: " << rightOne << "\n";
        }
        for (string s: leftShorts) {
            if(rightDisjunctionTB == s ){
                leftOne = allDisjunctions[i]->getRight()->toString();
//                cout  << "leftOne: " << leftOne << "\n";
            }
        }
        if (rightDisjunctionTB == rightShort){
            rightOne = allDisjunctions[i]->getRight()->toString();
//            cout << "rightOne: " << rightOne << "\n";
        }
        if (rightOne !="" && leftOne !=""){
//            cout << "JOIN STATEMENT\n";
//            cout  << "equalityCheck <" << leftOne << ", " << rightOne << ">\n";
            equalityChecks.push_back(make_pair(leftOne,rightOne));
            finalPredicate = allDisjunctions[i]->toString();
            
        }else{
//            cout << "REGULAR STATEMENT\n";
        }
        return make_pair(equalityChecks, finalPredicate);
    }
    void joinTables (MyDB_CatalogPtr myCatalog, MyDB_BufferManagerPtr myMgr, map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, string storageDir) {
        map <string, MyDB_TableReaderWriterPtr> TRWafterFilter;
        map<ExprTreePtr, bool> joinDisjunctions;
        queue<ExprTreePtr> allDisjunctionQ;
        // get all join tables and tbRW after filter, move tbRW to a vector
        TRWafterFilter = initiateTRWafterFilter(allTableReaderWriters,myMgr, storageDir);
        for (ExprTreePtr disjunction: allDisjunctions) {
            allDisjunctionQ.push(disjunction);
//            cout << "Current disjunction: " << disjunction->toString() << "\n";
            set<string> tables = disjunction->getTables();
            // if there are more than 1 tables in the disjunction, it is a join statement
            //      add these tables to joinTableSet
            if (tables.size() > 1) {
                joinDisjunctions[disjunction] = true;
            } else {
                TRWafterFilter = updateTRWafterFilter(TRWafterFilter, disjunction, myMgr, storageDir);
                joinDisjunctions[disjunction] = false;
            }
        }
        vector<string> leftShorts;
        string rightShort;
        MyDB_TableReaderWriterPtr leftTable;
        while(!allDisjunctionQ.empty()) {
            ExprTreePtr disjunction = allDisjunctionQ.front();
//            cout << "current disjunction: " << disjunction->toString() << "\n";
            if (joinDisjunctions[disjunction] == false) {
//                cout << "already deal with it or not a join\n";
                allDisjunctionQ.pop();
                continue;
            }
            string leftOne = *disjunction->getLeft()->getTables().begin();
            string rightOne = *disjunction->getRight()->getTables().begin();
            if (leftShorts.size() == 0) {
                // first two join, join directly
//                cout << "now join: " << leftOne << " with " << rightOne << "\n";
                leftTable = TRWafterFilter[leftOne];
                MyDB_TableReaderWriterPtr rightTable = TRWafterFilter[rightOne];
                leftShorts.push_back(leftOne);
                rightShort = rightOne;
                pair<map<ExprTreePtr, bool>, MyDB_TableReaderWriterPtr> result = twoTableJoin(myCatalog, myMgr, TRWafterFilter, leftTable, rightTable, leftShorts, rightShort, joinDisjunctions, storageDir);
                joinDisjunctions = result.first;
                leftTable = result.second;
                leftShorts.push_back(rightOne);
            } else {
                bool lin = false;
                bool rin = false;
                for (string s: leftShorts) {
                    if (leftOne == s) {
                        lin = true;
                    } else if (rightOne == s) {
                        rin = true;
                    }
                }
                if (lin && rin) {
                    cout << "shouldn't happen\n";
                } else if (lin && !rin) {
//                    cout << "join big table with " << rightOne << "\n";
                    MyDB_TableReaderWriterPtr rightTable = TRWafterFilter[rightOne];
                    rightShort = rightOne;
                    pair<map<ExprTreePtr, bool>, MyDB_TableReaderWriterPtr> result = twoTableJoin(myCatalog, myMgr, TRWafterFilter, leftTable, rightTable, leftShorts, rightShort, joinDisjunctions, storageDir);
                    joinDisjunctions = result.first;
                    leftTable = result.second;
                    leftShorts.push_back(rightOne);
                } else if (!lin && rin) {
//                    cout << "join big table with " << leftOne << "\n";
                    MyDB_TableReaderWriterPtr rightTable = TRWafterFilter[leftOne];
                    rightShort = leftOne;
                    pair<map<ExprTreePtr, bool>, MyDB_TableReaderWriterPtr> result = twoTableJoin(myCatalog, myMgr, TRWafterFilter, leftTable, rightTable, leftShorts, rightShort, joinDisjunctions, storageDir);
                    joinDisjunctions = result.first;
                    leftTable = result.second;
                    leftShorts.push_back(leftOne);
                } else {
//                    cout << "hop and deal with it later...\n";
                    allDisjunctionQ.pop();
                    allDisjunctionQ.push(disjunction);
                    continue;
                }
            }
        }
        
        
        //////
//        cout << "Selecting the following:\n";
        vector <ExprTreePtr> aggs;
        vector <string> projections;
        vector <string> groupings;
        vector <pair <MyDB_AggType, string>> aggsToCompute;
        MyDB_SchemaPtr mySchemaAggOut = make_shared <MyDB_Schema> ();
        bool groupFirst = true;
        if (valuesToSelect.at(0)->getType() != "regular") {
//            cout << "It seems that agg att first\n";
            groupFirst = false;
        }
        for (auto selected : valuesToSelect) {
            cout << "\t" << selected->toString () << "\n";
            if (selected->getType() == "regular") {
//                cout << "a type is regular" << selected->toString() << "\n";
                projections.push_back(selected->toString());
                groupings.push_back(selected->toString());
                vector<pair<string, string>> atts = selected->getAttsTables();
                int tablesSize = selected->getTables().size();
                int countSize = 0;
                int attsSize = atts.size();
                bool breakOrNot = false;
                bool isInt = true;
                string intName = "";
                for (pair<string, string> att: atts) {
                    if (breakOrNot == false){
                        cout << "atts:" << att.first << " in " << att.second << "\n";
                        for (auto a : leftShorts){
                            if (att.second == a) {
//                                cout << "schema append: " << att.first << ", type :" << leftTable->getTable()->getSchema()->getAttByName(att.first).second->toString() << "\n";
                                string type = leftTable->getTable()->getSchema()->getAttByName(att.first).second->toString();
                                if (type == "double" || type == "string"){
                                    mySchemaAggOut->appendAtt(make_pair (att.first, leftTable->getTable()->getSchema()->getAttByName(att.first).second));
                                    isInt = false;
                                    breakOrNot = true;
                                }else if (type == "int"){
//                                    mySchemaAggOut->appendAtt(make_pair (att.first, make_shared <MyDB_DoubleAttType> ()));
                                    intName = att.first;
                                    countSize+=1;
                                }
                            }
                        }
                        
                    }else{
                        break;
                    }
                }
                cout << "count Size " << countSize << "attSize "<<atts.size() <<"\n";
                if (countSize == atts.size()){
                        mySchemaAggOut->appendAtt(make_pair (intName, make_shared <MyDB_IntAttType> ()));
                    }else if (countSize != 0){
                        mySchemaAggOut->appendAtt(make_pair (intName, make_shared <MyDB_DoubleAttType> ()));
                    }
            } else {
                if (selected->getType() == "sum") {
//                    cout << "sum this: " <<  selected->getChild()->toString() << "\n";
                    if(selected->getChild()->toString() == "int[1]"){
                        aggsToCompute.push_back (make_pair (MyDB_AggType :: cnts, "int[0]"));
                        int number = rand() % 100;
                        string name = "cnt" + std::to_string(number);
                        mySchemaAggOut->appendAtt(make_pair (name, make_shared <MyDB_IntAttType>()));
                    }else{
                        aggsToCompute.push_back (make_pair (MyDB_AggType :: sums, selected->getChild()->toString()));
                        int number = rand() % 100;
                        string name = "sum" + std::to_string(number);
                        mySchemaAggOut->appendAtt(make_pair (name, make_shared <MyDB_DoubleAttType>()));
                    }
                } else if (selected->getType() == "avg") {
//                    cout << "avg this: " <<  selected->getChild()->toString() << "\n";
                    aggsToCompute.push_back (make_pair (MyDB_AggType :: avgs, selected->getChild()->toString()));
                    int number = rand() % 100;
                    string name = "avg" + std::to_string(number);
                    mySchemaAggOut->appendAtt(make_pair (name, make_shared <MyDB_DoubleAttType>()));
                }
            }
        }
        for (auto &p : mySchemaAggOut->getAtts ())
                cout << "schema in scan join: " << p.first << ", " << p.second->toString() << "\n";
        int outtableAggNumber = rand() % 100;
        string outTableAggName = "res" + std::to_string(outtableAggNumber) + "Out";
        MyDB_TablePtr myTableAggOut = make_shared <MyDB_Table> (outTableAggName, storageDir + "/" + outTableAggName + ".bin", mySchemaAggOut);
//        cout << "deleteMe push: " << myTableAggOut->getStorageLoc() << "\n";
        deleteMe.push_back(myTableAggOut->getStorageLoc());
        MyDB_TableReaderWriterPtr supplierTableAggOut = make_shared <MyDB_TableReaderWriter> (myTableAggOut, myMgr);
        string selectionPredicate="bool[true]";
        
        
        if (aggsToCompute.size() != 0){
            Aggregate myAgg(leftTable, supplierTableAggOut, aggsToCompute, groupings, selectionPredicate);
            myAgg.run(groupFirst);
        }else{
            RegularSelection myOp (leftTable, supplierTableAggOut, selectionPredicate, projections);
            myOp.run ();
        }

        MyDB_RecordPtr temp = supplierTableAggOut->getEmptyRecord ();
        MyDB_RecordIteratorAltPtr myIter = supplierTableAggOut->getIteratorAlt ();
        int count  = 0;
        while (myIter->advance ()) {
            myIter->getCurrent (temp);
            if (count < 30){
                cout << temp << "\n";
            }
            count++;
        }
        cout << "Total " << count << " rows in set.\n";
    }
    
    pair<map<ExprTreePtr, bool>, MyDB_TableReaderWriterPtr> twoTableJoin(MyDB_CatalogPtr myCatalog, MyDB_BufferManagerPtr myMgr, map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, MyDB_TableReaderWriterPtr leftTable, MyDB_TableReaderWriterPtr rightTable, vector<string> leftShorts, string rightShort, map<ExprTreePtr, bool> joinDisjunctions, string storageDir) {
        int t = clock();
        string finalSelectionPredicate = "";
        vector<pair <string, string>> equalityChecks;
        string leftSelectionPredicate ="";
        string rightSelectionPredicate ="";
        vector <string> projections;
        MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
        if (leftTable->getNumPages () < rightTable->getNumPages ()) {
            // schema contains all attributes from left table and righ table
            for (auto &p : leftTable->getTable ()->getSchema ()->getAtts ()){
                mySchemaOut->appendAtt (p);
                projections.push_back("["+p.first+"]");
            }
            
            for (auto &p : rightTable->getTable ()->getSchema ()->getAtts ()){
                mySchemaOut->appendAtt (p);
                projections.push_back("["+p.first+"]");
            }
        } else {
            // schema contains all attributes from left table and righ table
            for (auto &p : rightTable->getTable ()->getSchema ()->getAtts ()){
                mySchemaOut->appendAtt (p);
                projections.push_back("["+p.first+"]");
            }
            for (auto &p : leftTable->getTable ()->getSchema ()->getAtts ()){
                mySchemaOut->appendAtt (p);
                projections.push_back("["+p.first+"]");
            }
        }
        
        
        // output schema and projections
//        for (pair <string, MyDB_AttTypePtr> p : mySchemaOut->getAtts()) {
//            cout << "out schema: " << p.first << ", " << p.second->toString() << "\n";
//        }
//        for (string p : projections){
//            cout << "projections: " << p << "\n";
//        }
//        
        // finalselectionpredicate and equalitycheck
        
        string finalfirstPredicate = "";
        string finalsecondPredicate = "";
        string LeftfirstPredicate ="";
        string LeftsecondPredicate ="";
        string rightfirstPredicate ="";
        string rightsecondPredicate ="";
        string ltableName = leftTable->getTable()->getName();
        string rtableName = rightTable->getTable()->getName();
        
        
        ///////////////////////
//        cout << "leftTableName" <<ltableName << "\n";
//        cout << "rightTableName" <<rtableName << "\n";
//        cout << "leftShort: ";
//        for (string s : leftShorts){
//            cout << s << "  ";
//        }
//        cout << "\n";
//        cout << "rightShort " << rightShort << "\n";
//        ////////////////////
        
        
        
        if (allDisjunctions.size() > 0) {
            set<string> tables = allDisjunctions[0]->getTables();
            
            if(tables.size()>1){
                pair<vector<pair<string, string>>, string> joinPair =  dealWithJoinPredicate(0, equalityChecks, leftShorts, rightShort);
                equalityChecks = joinPair.first;
                finalfirstPredicate = joinPair.second;
                if (finalfirstPredicate != "") {
                    joinDisjunctions[allDisjunctions[0]] = false;
                }
                finalSelectionPredicate = finalfirstPredicate;
//                cout << "finalSelectionPredicate: " << finalSelectionPredicate << "\n";
            }else if (tables.size()==1){
                cout << "REGULAR STATEMENT...hop it\n";
            }
        }
        
        for (int i = 1; i < allDisjunctions.size(); i++ ) {
            set<string> tables = allDisjunctions[i]->getTables();
            if(tables.size()>1){
                //cout << "left one: " << *allDisjunctions[i]->getLeft()->getTables().begin() << "\n";
                //cout << "right one: " << *allDisjunctions[i]->getRight()->getTables().begin() << "\n";
                pair<vector<pair<string, string>>, string> joinPair = dealWithJoinPredicate(i, equalityChecks, leftShorts, rightShort);
                equalityChecks = joinPair.first;
                finalsecondPredicate = joinPair.second;
                if (finalsecondPredicate != "") {
                    joinDisjunctions[allDisjunctions[i]] = false;
                }
//                cout << "finalfirstPredicate: " << finalfirstPredicate <<"\n";
//                cout << "finalsecondPredicate: " << finalsecondPredicate << "\n";
                if (finalsecondPredicate !=""){
                    if (finalfirstPredicate == ""){
                        finalSelectionPredicate = finalsecondPredicate;
                    }else{
                        finalSelectionPredicate = "&& ( " + finalfirstPredicate + "," + finalsecondPredicate + ")";
                    }
                    finalfirstPredicate =finalSelectionPredicate;
                }else{
                    if (finalfirstPredicate == ""){
                        finalSelectionPredicate = "";
                    }else{
                        finalSelectionPredicate = finalfirstPredicate;
                    }
                    finalfirstPredicate =finalSelectionPredicate;
                }
//                cout << "finalSelectionPredicate: " << finalSelectionPredicate << "\n";
                
            }else if (tables.size()==1){
                cout << "REGULAR SELECTION...hop it\n";
            }
            
        }
        
        
        int outtableNumber = rand() % 100;
        string outTableName = "Join" + std::to_string(outtableNumber) + "Out";
        MyDB_TablePtr myTableOut = make_shared <MyDB_Table> (outTableName, storageDir + "/" + outTableName + ".bin", mySchemaOut);
//        cout << "deleteMe push: " << myTableOut->getStorageLoc() << "\n";
        deleteMe.push_back(myTableOut->getStorageLoc());
        MyDB_TableReaderWriterPtr tableOut = make_shared <MyDB_TableReaderWriter> (myTableOut, myMgr);
        
        
        if (leftSelectionPredicate == ""){
            leftSelectionPredicate = "bool[true]";
        }
        if (rightSelectionPredicate == ""){
            rightSelectionPredicate = "bool[true]";
        }
//        for (pair<string ,string> p: equalityChecks) {
//            cout << "pair " << p.first << "second "<< p.second << "\n";
//        }
//        cout << "finalSelectionPredicate" << finalSelectionPredicate << "\n";
//        cout << "leftSelectionPredicate" << leftSelectionPredicate << "\n";
//        cout << "rightSelectionPredicate" << rightSelectionPredicate << "\n";
//        cout << "ScanJoining...\n";
        ScanJoin myOp(leftTable, rightTable, tableOut,finalSelectionPredicate,projections,equalityChecks, leftSelectionPredicate,rightSelectionPredicate);
        myOp.run ();
//        cout << "Finish ScanJoin...\n";
//
//        MyDB_RecordPtr temp = tableOut->getEmptyRecord ();
//        MyDB_RecordIteratorAltPtr myIter = tableOut->getIteratorAlt ();
//        int count  = 0;
//        while (myIter->advance ()) {
//            myIter->getCurrent (temp);
////            cout << temp << "\n";
//            count++;
//        }
//        cout << "Total " << count << " rows in set. \n";
        return make_pair(joinDisjunctions, tableOut) ;
    }
    
    void getRA(MyDB_CatalogPtr myCatalog, MyDB_BufferManagerPtr myMgr, map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, string storageDir) {
//        cout << "All tables in current catalog:\n";
        map <string, MyDB_TablePtr> allTables = MyDB_Table::getAllTables(myCatalog);
//        for (std::map<string, MyDB_TablePtr>::iterator it=allTables.begin(); it!=allTables.end(); ++it)
//            std::cout << it->first << " => " << *it->second << "\n";
        /////////if there is only one table
//        cout << "From the following table:\n";
        if (tablesToProcess.size()==1){
            oneTableCase(myCatalog, myMgr, allTableReaderWriters, storageDir);
        } else {
            int t = clock();
            joinTables(myCatalog, myMgr, allTableReaderWriters, storageDir);
            t = clock() - t;
            float runningtime = (float)t / CLOCKS_PER_SEC;
            cout << "using " << runningtime << " seconds\n";
        }
        for (string dm: deleteMe) {
//            cout << "deleteMe: " << dm << "\n";
            if( remove(dm.c_str()) != 0 )
                perror( "Error deleting file" );
            else
                puts( "File successfully deleted" );
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
	
	void printSFWQuery (MyDB_CatalogPtr myCatalog, MyDB_BufferManagerPtr myMgr, map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, string storageDir) {
        myQuery.getRA(myCatalog, myMgr, allTableReaderWriters, storageDir);
		//myQuery.print ();
	}

	#include "FriendDecls.h"
};

#endif
