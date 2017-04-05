

#include <stdio.h>
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include <unordered_map>

#include "MyDB_Schema.h"
#include <iostream>

using namespace std;

SortMergeJoin :: SortMergeJoin (MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
               MyDB_TableReaderWriterPtr output, string finalSelectionPredicate,
               vector <string> projections,
               pair <string, string> equalityCheck, string leftSelectionPredicate,
                                string rightSelectionPredicate){
    this->leftInput = leftInput;
    this->rightInput = rightInput;
    this->output = output;
    this->finalSelectionPredicate = finalSelectionPredicate;
    this->projections = projections;
    this->equalityCheck = equalityCheck;
    this->leftSelectionPredicate = leftSelectionPredicate;
    this->rightSelectionPredicate = rightSelectionPredicate;
}

// execute the join
void SortMergeJoin:: run (){
    // get the left input record, and get the various functions over it
    MyDB_RecordPtr leftInputRec = leftInput->getEmptyRecord ();
    // get the right input record, and get the various functions over it
    MyDB_RecordPtr rightInputRec = rightInput->getEmptyRecord ();
    // and get the schema that results from combining the left and right records
    MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
    for (auto p : leftInput->getTable ()->getSchema ()->getAtts ())
        mySchemaOut->appendAtt (p);
    for (auto p : rightInput->getTable ()->getSchema ()->getAtts ())
        mySchemaOut->appendAtt (p);
    // get the combined record
    MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);
    
    // and make it a composite of the two input records
    combinedRec->buildFrom (leftInputRec, rightInputRec);
    
    
    // now, get the final predicate over it
    func finalPredicate = combinedRec->compileComputation (finalSelectionPredicate);
    // left: get the various functions whose output we'll hash
    func leftEqualities;
    leftEqualities.push_back (leftInputRec->compileComputation (p.first));
    
    // right: get the various functions whose output we'll hash
    func rightEqualities;
    rightEqualities.push_back (rightInputRec->compileComputation (p.second));
    
    //-----------Sort phase--------
    MyDB_TableReaderWriterPtr leftSorted = make_shared<MyDB_TableReaderWriter>();
    MyDB_RecordPtr lhsL = leftInput->getEmptyRecord();
    MyDB_RecordPtr rhsL = leftInput->getEmptyRecord();
    sort (64, leftInput, leftSorted, leftEqualities, lhsL, rhsL);
    
    MyDB_TableReaderWriterPtr rightSorted = make_shared<MyDB_TableReaderWriter>();
    MyDB_RecordPtr lhsR = leftInput->getEmptyRecord();
    MyDB_RecordPtr rhsR = leftInput->getEmptyRecord();
    sort (64, rightInput, rightSorted, rightEqualities, lhsR, rhsR);
    
    //---------Merge phase---------
    
    
}
