
#ifndef AGG_H
#define AGG_H

#include "MyDB_TableReaderWriter.h"
#include <string>
#include <utility>
#include <vector>

// This class encapulates a simple, hash-based aggregation + group by.  It does not
// need to work when there is not enough space in the buffer manager to store all of
// the groups.
using namespace std;

Aggregate (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
		vector <pair <MyDB_AggType, string>> aggsToCompute,
           vector <string> groupings, string selectionPredicate) {
    this->input = input;
    this->output = output;
    this->aggsToCompute = aggsToCompute;
    this->groupings = groupings;
    this->selectionPredicate = selectionPredicate;
}.
	
	// execute the aggregation
void run () {
    // this is the hash map we'll use to look up data... the key is the hashed value
    // of all of the records' join keys, and the value is a list of pointers were all
    // of the records with that hsah value are located
    unordered_map <size_t, vector <void *>> myHash;
    
    //create schema ?????????
    MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
    for (auto p : groupings) {
        pair <int, MyDB_AttTypePtr> atts = leftTable->getTable ()->getSchema ()->getAttByName(p);
        mySchemaOut->appendAtt (make_pair(p, atts.second));
    }
    for (auto p : aggsToCompute) {
        mySchemaOut->appendAtt (make_pair(p.second, p.first));
    }
    // get all data
    vector <MyDB_PageReaderWriter> allData;
    for (int i = 0; i < input->getNumPages (); i++) {
        MyDB_PageReaderWriter temp = input->getPinned (i);
        if (temp.getType () == MyDB_PageType :: RegularPage){
            allData.push_back (input->getPinned (i));
        }
    }
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();
    func pred = inputRec->compileComputation (selectionPredicate);
    
    // get all grouping func
    vector <func> groupingFunc;
    for (auto p : groupings) {
        groupingFunc.push_back (inputRec->compileComputation (p));
    }
    
    MyDB_RecordPtr groupedRec = make_shared <MyDB_Record> (mySchemaOut);
    while (myIter->advance ()) {
        
        // hash the current record
        myIter->getCurrent (inputRec);
        
        // see if it is accepted by the preicate
        if (!pred ()->toBool ()) {
            continue;
        }
        
        // compute its hash
        size_t hashVal = 0;
        for (auto f : groupingFunc) {
            hashVal ^= f ()->hash ();
        }
        
        // see if it is in the hash table
        if (myHash.count (hashVal) == 0) {
            //not in hash, add (newkey, newval)
            myHash [hashVal].push_back (????);
        } else {
            //In hash? val+=newval
            //If you need to update the aggregate, you can use fromBinary () on the record to reconstitute it, then change the value as needed (using a pre-compiled func object) and then use toBinary () to write it back again.
            groupedRec->fromBinary (hashVal);
            
        }
        
        
    }
    
    
}

#endif
