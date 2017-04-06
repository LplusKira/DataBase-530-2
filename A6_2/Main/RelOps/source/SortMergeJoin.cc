

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

    vector <func> finalComputations;
    for (string s : projections) {
        finalComputations.push_back (combinedRec->compileComputation (s));
    }
    // and make it a composite of the two input records
    combinedRec->buildFrom (leftInputRec, rightInputRec);
    
//    MyDB_RecordPtr leftInputRec2 = leftInput->getEmptyRecord ();
//    MyDB_RecordPtr rightInputRec2 = rightInput->getEmptyRecord ();
//    combinedRecL->buildFrom (leftInputRec, leftInputRec2);
//    combinedRecR->buildFrom (rightInputRec, rightInputRec2);
    
    // now, get the final predicate over it
    func finalPredicate = combinedRec->compileComputation (finalSelectionPredicate);
    // get some compare func
    func leftSmaller = combinedRec->compileComputation (" < (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func rightSmaller = combinedRec->compileComputation (" > (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func areEqual = combinedRec->compileComputation (" == (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    
    //func smallerL = combinedRecL->compileComputation (" < (" + equalityCheck.first + ", " + equalityCheck.first + ")");
    //func equalL = combinedRecL->compileComputation (" == (" + equalityCheck.first + ", " + equalityCheck.first + ")");
    //func biggerL = combinedRecL->compileComputation (" > (" + equalityCheck.first + ", " + equalityCheck.first + ")");
    
    //func smallerR = combinedRecR->compileComputation (" < (" + equalityCheck.second + ", " + equalityCheck.second + ")");
    //func equalR = combinedRecR->compileComputation (" == (" + equalityCheck.second + ", " + equalityCheck.second + ")");
    //func biggerR = combinedRecR->compileComputation (" > (" + equalityCheck.second + ", " + equalityCheck.second + ")");
    
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
    function <bool ()> f1 = buildRecordComparator(lhsL, rhsL, " < (" + equalityCheck.first + ", " + equalityCheck.first + ")");
    sort (64, leftInput, leftSorted, f1, lhsL, rhsL);
    
    MyDB_TableReaderWriterPtr rightSorted = make_shared<MyDB_TableReaderWriter>();
    MyDB_RecordPtr lhsR = leftInput->getEmptyRecord();
    MyDB_RecordPtr rhsR = leftInput->getEmptyRecord();
    function <bool ()> f2 = buildRecordComparator(lhsR, rhsR, " < (" + equalityCheck.second + ", " + equalityCheck.second + ")");
    sort (64, rightInput, rightSorted, f2, lhsR, rhsR);
    
    //---------Merge phase---------
    // now get the predicate
    func leftPred = leftInputRec->compileComputation (leftSelectionPredicate);
    // now get the predicate
    func rightPred = rightInputRec->compileComputation (rightSelectionPredicate);
    
    MyDB_RecordIteratorAltPtr iterL = leftSorted->getIteratorAlt ();
    MyDB_RecordPtr recL = leftSorted->getEmptyRecord ();
    MyDB_RecordIteratorAltPtr iterR = rightSorted->getIteratorAlt ();
    MyDB_RecordPtr recR = rightSorted->getEmptyRecord ();
    iterL->advance();
    iterR->advance();
    while (true) {
        iterL->getCurrent(recL);
        iterR->getCurrent(recR);
        // see if it is accepted by the preicate
        if (!leftPred ()->toBool ()) {
            if (!iterL->advance()) {
                break;
            }
        }
        // see if it is accepted by the preicate
        if (!rightPred ()->toBool ()) {
            if (iterR->advance()) {
                break;
            }
        }
        if (!finalPredicate ()->toBool ()) {
            if (!iterL->advance()) {
                break;
            }
            if (iterR->advance()) {
                break;
            }
        } else {
            if (leftSmaller ()->toBool ()) {
                // left small
                if (!iterL->advance()) {
                    break;
                }
            } else if (rightSmaller ()->toBool()){
                // right small
                if (iterR->advance()) {
                    break;
                }
            } else if (areEqual()->toBool()) {
                // equal
                // push all equal left to vector
                vector<MyDB_RecordPtr> leftBox;
                leftBox.push_back(recL);
                if (!iterL->advance()) {
                    break;
                }
                
                while (true) {
                    function <bool ()> f = buildRecordComparator(leftBox.front(), recL, " < (" + equalityCheck.first + ", " + equalityCheck.first + ")");
                    if (f()) {
                        leftBox.push_back(recL);
                        if (!iterL->advance()) {
                            break;
                        }
                    } else {
                        break;
                    }
                    
                }
                // push all equal right to vector
                vector<MyDB_RecordPtr> rightBox;
                rightBox.push_back(recR);
                if (!iterR->advance()) {
                    break;
                }
                while (true) {
                    function <bool ()> f = buildRecordComparator(rightBox.front(), recR, " < (" + equalityCheck.second + ", " + equalityCheck.second + ")");
                    if (f()) {
                        rightBox.push_back(recR);
                        if (!iterR->advance()) {
                            break;
                        }
                    } else {
                        break;
                    }
                    
                }
                mergRecs(leftBox, rightBox, output, myschemaOut);
                if (!iterL->advance()) {
                    break;
                }
                if (iterR->advance()) {
                    break;
                }
            }
        }
    }
   
}

void SortMergeJoin:: mergeRecs (vector<MyDB_RecordPtr> left, vector<MyDB_RecordPtr> right, MyDB_TableReaderWriterPtr output, MyDB_SchemaPtr mySchemaOut, vector <func> finalComputations){
    for(MyDB_RecordPtr leftRec:left){
        for(MyDB_RecordPtr rightRec:right){
            MyDB_RecordPtr outputRec = output->getEmptyRecord ();
            if (finalSelectionPredicate ()->toBool ()) {
                
                // run all of the computations
                int i = 0;
                for (auto f : finalComputations) {
                    outputRec->getAtt (i++)->set (f());
                }
                outputRec->recordContentHasChanged ();
                output->append (outputRec);	
            }

            output->append (combinedRec);
            
        }
    }
}
