

#include <stdio.h>
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include <unordered_map>
#include "Sorting.h"
#include "MyDB_AttType.h"
#include "MyDB_BufferManager.h"
#include "MyDB_Catalog.h"
#include "MyDB_Page.h"
#include "MyDB_Record.h"
#include "MyDB_Table.h"

#include "MyDB_Schema.h"
#include <iostream>
#include <vector>

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
    // now, get the final predicate over it
    
    // and make it a composite of the two input records
    combinedRec->buildFrom (leftInputRec, rightInputRec);
    
    // get some compare func
    func leftSmaller = combinedRec->compileComputation (" < (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func rightSmaller = combinedRec->compileComputation (" > (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func areEqual = combinedRec->compileComputation (" == (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    
    // left: get the various functions whose output we'll hash
    
    func leftEqualities;
    leftEqualities = leftInputRec->compileComputation (equalityCheck.first);
    
    // right: get the various functions whose output we'll hash
    func rightEqualities;
    rightEqualities = rightInputRec->compileComputation (equalityCheck.second);
    
    //-----------Sort phase--------
    MyDB_TablePtr leftTable = make_shared <MyDB_Table> ("leftSorted", "leftSorted.bin", leftInput->getTable ()->getSchema ());
    MyDB_BufferManagerPtr leftMgr = leftInput->getBufferMgr();
    MyDB_TableReaderWriter leftSorted (leftTable, leftMgr);
    MyDB_RecordPtr lhsL = leftInput->getEmptyRecord();
    MyDB_RecordPtr rhsL = leftInput->getEmptyRecord();
    function <bool ()> f1 = buildRecordComparator(lhsL, rhsL, equalityCheck.first);
    sort (64, *leftInput, leftSorted, f1, lhsL, rhsL);
    
    
    
    
    MyDB_TablePtr rightTable = make_shared <MyDB_Table> ("rightSorted", "rightSorted.bin", rightInput->getTable ()->getSchema ());
    MyDB_BufferManagerPtr rightMgr = rightInput->getBufferMgr();
    MyDB_TableReaderWriter rightSorted (rightTable, rightMgr);
    MyDB_RecordPtr lhsR = rightInput->getEmptyRecord();
    MyDB_RecordPtr rhsR = rightInput->getEmptyRecord();
    function <bool ()> f2 = buildRecordComparator(lhsR, rhsR, equalityCheck.second);
    sort (64, *rightInput, rightSorted, f2, lhsR, rhsR);
    
    //    cout << "\n\nleft sort result: \n";
    //    MyDB_RecordIteratorAltPtr iter = leftSorted.getIteratorAlt ();
    //    MyDB_RecordPtr rec = leftSorted.getEmptyRecord ();
    //    while (iter->advance()) {
    //        iter->getCurrent(rec);
    //        cout << rec << "\n";
    //    }
    //    cout << "\n\nright sort result: \n";
    //    MyDB_RecordIteratorAltPtr iter2 = rightSorted.getIteratorAlt ();
    //    MyDB_RecordPtr rec2 = rightSorted.getEmptyRecord ();
    //    while (iter2->advance()) {
    //        iter2->getCurrent(rec2);
    //        cout << rec2 << "\n";
    //    }
    //---------Merge phase---------
    
    
    MyDB_RecordIteratorAltPtr iterL = leftSorted.getIteratorAlt ();
    MyDB_RecordPtr recL = leftSorted.getEmptyRecord ();
    MyDB_RecordIteratorAltPtr iterR = rightSorted.getIteratorAlt ();
    MyDB_RecordPtr recR = rightSorted.getEmptyRecord ();
    
    vector<MyDB_PageReaderWriter> leftBox;
    vector<MyDB_PageReaderWriter> rightBox;
    bool leftMove = true;
    bool rightMove = true;
    bool leftEnd = false;
    bool rightEnd = false;
    while (!leftEnd || !rightEnd){
//        cout << "\n\n\n";
        iterL->getCurrent(recL);
        iterR->getCurrent(recR);
        //        cout << "right rec" << recR << "\n";
        //MyDB_RecordPtr recLTemp = leftSorted.getEmptyRecord ();
        //MyDB_RecordPtr recRTemp = rightSorted.getEmptyRecord ();
        //        iterL->getCurrent(recLTemp);
        //        iterR->getCurrent(recRTemp);
        if(leftBox.size()==0||rightBox.size()==0){
//            cout << "empty vec\n";
            if (leftMove) {
                cout << "left rec" << recL << "\n";
                cout << "left move\n";
                // now get the predicate
                func leftPred = recL->compileComputation (leftSelectionPredicate);
                
                int checkLeft=checkSingleAcceptance(leftPred,iterL,recL);
//                cout << "###############\n";
                if(checkLeft==1){
                    // no more records
//                    cout << "left no more records\n";
                    leftEnd = true;
                    leftMove = false;
                    continue;
                }else if(checkLeft==2){
//                    cout << "left not accepcted\n";
                    leftMove = true;
                    // not accepted
                    continue;
                }else {
                    leftMove = false;
                }
            }
            if (rightMove) {
//                cout << "right move\n";
                // now get the predicate
                func rightPred = recR->compileComputation (rightSelectionPredicate);
                int checkRight=checkSingleAcceptance(rightPred,iterR,recR);
                if(checkRight==1){
                    // no more records
//                    cout << "right no more records\n";
                    rightEnd = true;
                    rightMove = false;
                    continue;
                }else if(checkRight==2){
                    // not accepted
//                    cout << "right not accepcted\n";
                    rightMove = true;
                    continue;
                } else {
                    rightMove = false;
                }
            }
            // get the combined record
            MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);
            // now, get the final predicate over it
            
            // and make it a composite of the two input records
            combinedRec->buildFrom (recL, recR);
            
            // get some compare func
            func leftSmaller = combinedRec->compileComputation (" < (" + equalityCheck.first + ", " + equalityCheck.second + ")");
            func rightSmaller = combinedRec->compileComputation (" > (" + equalityCheck.first + ", " + equalityCheck.second + ")");
            func areEqual = combinedRec->compileComputation (" == (" + equalityCheck.first + ", " + equalityCheck.second + ")");
            if(leftSmaller ()->toBool ()){
                if(!iterL->advance()){
//                    cout << "left no more records\n";
                    leftEnd = true;
                    continue;
                }else{
//                    cout << "left smaller, jump to next record\n";
                    leftMove = true;
                    rightMove = false;
                    continue;
                }
            }else if(rightSmaller ()->toBool ()){
                if(!iterR->advance()){
//                    cout << "right no more records\n";
                    rightEnd = true;
                    rightMove = false;
                    continue;
                }else{
//                    cout << "right smaller, jump to next record\n";
                    leftMove = false;
                    rightMove = true;
                    continue;
                }
            }else if (areEqual() ->toBool()){
                cout << "equal ones, push to vec\n";
                cout << "right vec" << recR<<"\n";
                MyDB_PageReaderWriter prwL(*leftInput->getBufferMgr());
                MyDB_PageReaderWriter prwR (*rightInput->getBufferMgr());
                prwL.append(recL);
                prwR.append(recR);
                leftBox.push_back(prwL);
                rightBox.push_back(prwR);
                if(!iterL->advance()){
//                    cout << "left no more records\n";
                    leftEnd = true;
                    leftMove = false;
                }else{
                    leftMove=true;
                }
                if(!iterR->advance()){
//                    cout << "right no more records\n";
                    rightEnd= true;
                    rightMove=false;
                }else{
                    rightMove=true;
                }
//                cout << "both jump to next\n";
//                leftMove = true;
//                rightMove = true;
            }
            
        }else{
//            cout << "non-empty vec\n";
            int stillNum=0;
            if (leftMove) {
                //int rightNextState=nextState(equalityCheck.second, rightBox,iterR,recR,rightPred);
                MyDB_RecordPtr temp = leftSorted.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = leftBox.front().getIteratorAlt ();
                myIter->getCurrent (temp);
                //cout << "L vec rec: " << temp << "\n";
                
                function <bool ()> f1 = buildRecordComparator(temp, recL, equalityCheck.first);
                function <bool ()> f2 = buildRecordComparator(recL, temp, equalityCheck.first);
                if(!f1() && !f2()){
//                    cout << "L: rec = vec.rec\n";
                    // now get the predicate
                    func leftPred = recL->compileComputation (leftSelectionPredicate);
                    int check = checkSingleAcceptance(leftPred,iterL,recL);
                    if(check ==1){
                        leftEnd = true;
                        leftMove = false;;
                    }else if(check==2){
                        continue;
                    } else {
//                        cout << "L: push...\n";
                        if (!leftBox.back().append(recL)) {
                            MyDB_PageReaderWriter newPage(*leftInput->getBufferMgr());
                            newPage.append(recL);
                            leftBox.push_back(newPage);
                        }
                        if(!iterL->advance()){
                            leftEnd = true;
                            leftMove = false;
                        }
                        
                    }
                }else{
//                    cout << "L: rec != vec.rec\n";
//                    cout << "L still.\n";
                    stillNum++;
                    leftMove = false;
                }
            }
            if (rightMove) {
                
                //int rightNextState=nextState(equalityCheck.second, rightBox,iterR,recR,rightPred);
                MyDB_RecordPtr temp = rightSorted.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = rightBox.front().getIteratorAlt ();
                myIter->getCurrent (temp);
                //cout << "R vec rec: " << temp << "\n";
                
                function <bool ()> f1 = buildRecordComparator(temp, recR, equalityCheck.second);
                function <bool ()> f2 = buildRecordComparator(recR, temp, equalityCheck.second);
                if(!f1() && !f2()){
//                    cout << "R: rec = vec.rec\n";
                    // now get the predicate
                    func rightPred = recR->compileComputation (rightSelectionPredicate);
                    int check = checkSingleAcceptance(rightPred,iterR,recR);
                    if(check ==1){
                        rightEnd = true;
                        rightMove = false;
                    }else if(check==2){
                        continue;
                    } else {
//                        cout << "R: push...\n";
                        if (!rightBox.back().append(recR)) {
                            MyDB_PageReaderWriter newPage (*rightInput->getBufferMgr());
                            newPage.append(recR);
                            rightBox.push_back(newPage);
                        }
                        if(!iterR->advance()){
                            rightEnd = true;
                            rightMove = false;
                        }
                    }
                }else{
//                    cout << "R: rec != vec.rec\n";
//                    cout << "R: right still.\n";
                    stillNum++;
                    rightMove = false;
                }
            }
//            cout << "L vec:\n";
            
//            for (MyDB_PageReaderWriter p: leftBox) {
//                MyDB_RecordPtr temp = leftSorted.getEmptyRecord ();
//                MyDB_RecordIteratorAltPtr myIter = p.getIteratorAlt ();
//                while (myIter->advance ()) {
//                    myIter->getCurrent (temp);
//                    cout << temp << "\n";
//                }
//            }
//            cout << "R vec: \n";
//            for (MyDB_PageReaderWriter p: rightBox) {
//                MyDB_RecordPtr temp = rightSorted.getEmptyRecord ();
//                MyDB_RecordIteratorAltPtr myIter = p.getIteratorAlt ();
//                while (myIter->advance ()) {
//                    myIter->getCurrent (temp);
//                    cout << temp << "\n";
//                }
//                
//            }
            if(!rightMove && !leftMove){
                MyDB_RecordPtr recl = leftSorted.getEmptyRecord ();
                MyDB_RecordPtr recr = rightSorted.getEmptyRecord ();
//                cout << "ready to merge\n";
                mergeRecs(recl, recr, leftBox, rightBox, output,mySchemaOut);
                leftBox.clear();
                rightBox.clear();
//                cout << "finish clear\n";
                leftMove = true;
                rightMove = true;
            }
        }
    }
    
    
}

int SortMergeJoin ::checkSingleAcceptance(func pred, MyDB_RecordIteratorAltPtr iter, MyDB_RecordPtr rec) {
//    cout << "######checkSingleAcceptance:\n";
    iter->getCurrent(rec);
    //        cout << "record: " << rec << "\n";
    
    // see if it is accepted by the preicate
    if (!pred ()->toBool ()) {
        cout << "not qualified, move to next\n";
        if (!iter->advance()) {
            return 1;//end of the join
        } else {
            iter->getCurrent(rec);
            //                cout << "now rec: " << rec << "\n";
            return 2;// continue
        }
        
    } else {
//        cout << "qualify\n";
        
        return 3;//keep on
    }
}
int SortMergeJoin ::checkBothAcceptance(MyDB_RecordIteratorAltPtr iterL, MyDB_RecordPtr recL, MyDB_RecordIteratorAltPtr iterR, MyDB_RecordPtr recR, func pred) {
//    cout << "######checkBothAcceptance:\n";
    iterL->getCurrent(recL);
    iterR->getCurrent(recR);
    if (!pred ()->toBool ()) {
//        cout << "both not qualified\n";
        if (!iterL->advance()) {
            return 1;//end of join
        }
        if (iterR->advance()) {
            return 1; //end of join
        }
        iterL->getCurrent(recL);
        iterR->getCurrent(recR);
        return 2; //continue
    } else {
        return 3;//keep on
    }
}
void SortMergeJoin:: mergeRecs (MyDB_RecordPtr leftRec, MyDB_RecordPtr rightRec, vector<MyDB_PageReaderWriter> left, vector<MyDB_PageReaderWriter> right, MyDB_TableReaderWriterPtr output,MyDB_SchemaPtr mySchemaOut ){
    MyDB_RecordIteratorAltPtr iterL = getIteratorAlt(left);
    
    while(iterL->advance()){
        iterL->getCurrent(leftRec);
        //cout << "leftRec" << leftRec <<"\n";
        MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);
        combinedRec->buildFrom (leftRec, rightRec);
        // now, get the final predicate over it
        func finalPredicate = combinedRec->compileComputation (this->finalSelectionPredicate);
        
        // and get the final set of computatoins that will be used to buld the output record
        vector <func> finalComputations;
        for (string s : this->projections) {
            finalComputations.push_back (combinedRec->compileComputation (s));
        }
        MyDB_RecordIteratorAltPtr iterR = getIteratorAlt(right);
        
        while(iterR->advance()){
            iterR->getCurrent(rightRec);
            //cout << "rightRec" << rightRec <<"\n";
            MyDB_RecordPtr outputRec = output->getEmptyRecord ();
            if (finalPredicate ()->toBool ()) {
//                cout << "qualify for final predicate\n";
                // run all of the computations
                int i = 0;
                for (auto f : finalComputations) {
                    outputRec->getAtt (i++)->set (f());
                }
                
                outputRec->recordContentHasChanged ();
//                cout << "output: " << outputRec << "\n";
                output->append (outputRec);
            }
            
        }
    }
}
