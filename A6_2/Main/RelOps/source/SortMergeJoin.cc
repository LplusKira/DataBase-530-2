

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
    
//    MyDB_RecordIteratorAltPtr myIter = rightSorted.getIteratorAlt ();
//    
//    MyDB_RecordPtr rec1 = rightInput->getEmptyRecord ();
//    while (myIter->advance ()) {
//        myIter->getCurrent (rec1);
//        cout << rec1 << "\n";
//    }
    //---------Merge phase---------
    
    
    MyDB_RecordIteratorAltPtr iterL = leftSorted.getIteratorAlt ();
    MyDB_RecordPtr recL = leftSorted.getEmptyRecord ();
    MyDB_RecordIteratorAltPtr iterR = rightSorted.getIteratorAlt ();
    MyDB_RecordPtr recR = rightSorted.getEmptyRecord ();
    // now get the predicate
    func leftPred = recL->compileComputation (leftSelectionPredicate);
    // now get the predicate
    func rightPred = recR->compileComputation (rightSelectionPredicate);
    iterL->advance();
    iterR->advance();
//    bool ifcont = true;
//    while (ifcont) {
//        int c1 = checkSingleAcceptance(leftPred, iterL, recL);
//        if (c1 == 2) {
//            continue;
//        } else if (c1 == 1){
//            break;
//        }
//        int c2 = checkSingleAcceptance(rightPred, iterR, recR);
//        if (c2 == 2) {
//            continue;
//        } else if (c2 == 1){
//            break;
//        }
////        iterL->getCurrent(recL);
////        iterR->getCurrent(recR);
////        cout << "left record: \n";
////        cout << recL << "\n\n";
////        cout << "right record: \n";
////        cout << recR << "\n\n";
////
////        // see if it is accepted by the preicate
////        if (!leftPred ()->toBool ()) {
////            cout << "left not qualified\n";
////            if (!iterL->advance()) {
////                break;
////            } else {
////                continue;
////            }
////            
////        }
////        // see if it is accepted by the preicate
////        if (!rightPred ()->toBool ()) {
////            cout << "right not qualified\n";
////            if (!iterR->advance()) {
////                break;
////            } else {
////                continue;
////            }
////        }
////        if (!finalPredicate ()->toBool ()) {
////            cout << "both not qualified\n";
////            if (!iterL->advance()) {
////                break;
////            }
////            if (iterR->advance()) {
////                break;
////            }
////            continue;
////        } else {
//        int bothAccept = checkBothAcceptance(iterL, recL, iterL, recL, finalPredicate);
//        if (bothAccept == 1) {
//            break;
//        } else if (bothAccept == 2) {
//            continue;
//        }else {
//            cout << "START...\n";
//            if (leftSmaller ()->toBool ()) {
//                // left small
//                cout << "left small\n";
//                if (!iterL->advance()) {
//                    break;
//                } else {
//                    continue;
//                }
//            } else if (rightSmaller ()->toBool()){
//                // right small
//                cout << "right small\n";
//                if (iterR->advance()) {
//                    break;
//                } else {
//                    continue;
//                }
//            } else if (areEqual()->toBool()) {
//                // equal
//                // push all equal left to vector
//                cout << "equal\n";
//                vector<MyDB_RecordPtr> leftBox;
//                leftBox.push_back(recL);
//                if (!iterL->advance()) {
//                    break;
//                }
//                while (true) {
//                    iterL->getCurrent(recL);
//                    iterR->getCurrent(recR);
//                    int singleAcc = checkSingleAcceptance(leftPred, iterL, recL);
//                    int bothAcc = checkBothAcceptance(iterL, recL, iterR, recR, finalPredicate);
//                    if (singleAcc!=3 || bothAcc!=3) {
//                        if (!iterL->advance()) {
//                            break;
//                        } else {
//                            continue;
//                        }
//                    }
//                    function <bool ()> fl = buildRecordComparator(leftBox.front(), recL, equalityCheck.first);
//                    function <bool ()> fr = buildRecordComparator( recL,leftBox.front(), equalityCheck.first);
//                    if (!fl()&&!fr()) {
//                        cout << "left same...\n";
//                        leftBox.push_back(recL);
//                        if (!iterL->advance()) {
//                            break;
//                        } else {
//                            continue;
//                        }
//                    } else {
//                        break;
//                    }
//                }
//                for (MyDB_RecordPtr r : leftBox) {
//                    cout << r << "\n";
//                }
//                // push all equal right to vector
//                vector<MyDB_RecordPtr> rightBox;
//                rightBox.push_back(recR);
//                if (!iterR->advance()) {
//                    break;
//                }
//                
//                while (true) {
//                    iterL->getCurrent(recL);
//                    iterR->getCurrent(recR);
//                    bool singleAcc = checkSingleAcceptance(rightPred, iterR, recR);
//                    bool bothAcc = checkBothAcceptance(iterL, recL, iterR, recR, finalPredicate);
//                    if (!singleAcc || !bothAcc) {
//                        if (!iterR->advance()) {
//                            break;
//                        } else {
//                            continue;
//                        }
//                    }
//                    function <bool ()> fl = buildRecordComparator(rightBox.front(), recR, equalityCheck.second);
//                    function <bool ()> fr = buildRecordComparator(recR, rightBox.front(), equalityCheck.second);
//                    if (!fl() && !fr()) {
//                        rightBox.push_back(recR);
//                        if (!iterR->advance()) {
//                            break;
//                        } else {
//                            continue;
//                        }
//                    } else {
//                        break;
//                    }
//                    
//                }
//                for (MyDB_RecordPtr r : leftBox) {
//                    cout << r << "\n";
//                }
//                mergeRecs(leftBox, rightBox, output, mySchemaOut,finalComputations, finalPredicate);
//
//                if (!iterL->advance()) {
//                    break;
//                }
//                if (iterR->advance()) {
//                    break;
//                }
//            }
//        }
//    }
    vector<MyDB_PageReaderWriter> leftBox;
    vector<MyDB_PageReaderWriter> rightBox;
    bool leftMove = true;
    bool rightMove = true;
    while (true){
        cout << "\n\n\n";
        iterL->getCurrent(recL);
        iterR->getCurrent(recR);
        cout << "left rec" << recL << "\n";
        cout << "right rec" << recR << "\n";
        MyDB_RecordPtr recLTemp = leftSorted.getEmptyRecord ();
        MyDB_RecordPtr recRTemp = rightSorted.getEmptyRecord ();
        iterL->getCurrent(recLTemp);
        iterR->getCurrent(recRTemp);
        if(leftBox.size()==0||rightBox.size()==0){
            cout << "empty vec\n";
            if (leftMove) {
                int checkLeft=checkSingleAcceptance(leftPred,iterL,recL);
                cout << "###############\n";
                if(checkLeft==1){
                    // no more records
                    cout << "left no more records\n";
                    break;
                }else if(checkLeft==2){
                    cout << "left not accepcted\n";
                    leftMove = true;
                    // not accepted
                    continue;
                }else {
                    leftMove = false;
                }
            }
            if (rightMove) {
                int checkRight=checkSingleAcceptance(rightPred,iterR,recR);
                if(checkRight==1){
                    // no more records
                    cout << "right no more records\n";
                    break;
                }else if(checkRight==2){
                    // not accepted
                    cout << "right not accepcted\n";
                    rightMove = true;
                    continue;
                } else {
                    rightMove = false;
                }
            }
            
            if(leftSmaller ()->toBool ()){
                if(!iterL->advance()){
                    cout << "left no more records\n";
                    break;
                }else{
                    cout << "left smaller, jump to next record\n";
                    leftMove = true;
                    rightMove = false;
                    continue;
                }
            }else if(rightSmaller ()->toBool ()){
                if(!iterL->advance()){
                    cout << "right no more records\n";
                    break;
                }else{
                    cout << "right smaller, jump to next record\n";
                    leftMove = false;
                    rightMove = true;
                    continue;
                }
            }else{
                cout << "equal ones, push to vec\n";
                MyDB_PageReaderWriter prwL(*leftInput->getBufferMgr());
                MyDB_PageReaderWriter prwR (*rightInput->getBufferMgr());
                prwL.append(recRTemp);
                prwR.append(recLTemp);
                rightBox.push_back(prwL);
                leftBox.push_back(prwR);
                if(!iterL->advance()){
                    cout << "left no more records\n";
                    break;
                }
                if(!iterR->advance()){
                    cout << "right no more records\n";
                    break;
                }
                cout << "both jump to next\n";
                leftMove = true;
                rightMove = true;
            }
            
        }else{
            cout << "non-empty vec\n";
            int stillNum=0;
            if (leftMove) {
                //int rightNextState=nextState(equalityCheck.second, rightBox,iterR,recR,rightPred);
                MyDB_RecordPtr temp = leftSorted.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = leftBox.front().getIteratorAlt ();
                myIter->getCurrent (temp);
                cout << "vec rec: " << temp << "\n";
                
                function <bool ()> f1 = buildRecordComparator(temp, recL, equalityCheck.first);
                function <bool ()> f2 = buildRecordComparator(recL, temp, equalityCheck.first);
                if(!f1() && !f2()){
                    cout << "L: rec = vec.rec\n";
                    int check = checkSingleAcceptance(leftPred,iterL,recL);
                    if(check ==1){
                        if(leftBox.size()!=0&&rightBox.size()!=0){
                            MyDB_RecordPtr recl = leftSorted.getEmptyRecord ();
                            MyDB_RecordPtr recr = rightSorted.getEmptyRecord ();
                            mergeRecs(recl, recr, leftBox, rightBox, output, finalComputations, finalPredicate);
                        }
                        break;
                    }else if(check==2){
                        continue;
                    } else {
                        cout << "L: push...\n";
                        if (!leftBox.back().append(recL)) {
                            MyDB_PageReaderWriter newPage(*leftInput->getBufferMgr());
                            newPage.append(recL);
                            leftBox.push_back(newPage);
                        }
                        if(!iterL->advance()){
                            if(leftBox.size()!=0&&rightBox.size()!=0){
                                MyDB_RecordPtr recl = leftSorted.getEmptyRecord ();
                                MyDB_RecordPtr recr = rightSorted.getEmptyRecord ();
                                mergeRecs(recl, recr, leftBox, rightBox, output, finalComputations, finalPredicate);

                            }
                        }
                        
                    }
                }else{
                    cout << "L: rec != vec.rec\n";
                    cout << "L still.\n";
                    stillNum++;
                    leftMove = false;
                }
            }
            if (rightMove) {
                
                //int rightNextState=nextState(equalityCheck.second, rightBox,iterR,recR,rightPred);
                MyDB_RecordPtr temp = rightSorted.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = rightBox.front().getIteratorAlt ();
                myIter->getCurrent (temp);
                cout << "vec rec: " << temp << "\n";

                function <bool ()> f1 = buildRecordComparator(temp, recR, equalityCheck.second);
                function <bool ()> f2 = buildRecordComparator(recR, temp, equalityCheck.second);
                if(!f1() && !f2()){
                    cout << "R: rec = vec.rec\n";
                    int check = checkSingleAcceptance(rightPred,iterR,recR);
                    if(check ==1){
                        if(leftBox.size()!=0&&rightBox.size()!=0){
                            MyDB_RecordPtr recl = leftSorted.getEmptyRecord ();
                            MyDB_RecordPtr recr = rightSorted.getEmptyRecord ();
                            mergeRecs(recl, recr, leftBox, rightBox, output, finalComputations, finalPredicate);
                        }
                        break;
                    }else if(check==2){
                        continue;
                    } else {
                        cout << "R: push...\n";
                        if (!rightBox.back().append(recR)) {
                            MyDB_PageReaderWriter newPage (*rightInput->getBufferMgr());
                            newPage.append(recR);
                            leftBox.push_back(newPage);
                        }
                        if(!iterR->advance()){
                            if(leftBox.size()!=0&&rightBox.size()!=0){
                                MyDB_RecordPtr recl = leftSorted.getEmptyRecord ();
                                MyDB_RecordPtr recr = rightSorted.getEmptyRecord ();
                                mergeRecs(recl, recr, leftBox, rightBox, output, finalComputations, finalPredicate);

                            }
                        }
                    }
                }else{
                    cout << "R: rec != vec.rec\n";
                    cout << "R: right still.\n";
                    stillNum++;
                    rightMove = false;
                }
            }
            
            cout << "L vec:\n";
            MyDB_RecordPtr temp = leftSorted.getEmptyRecord ();
            for (MyDB_PageReaderWriter p: leftBox) {
                MyDB_RecordIteratorAltPtr myIter = p.getIteratorAlt ();
                while (myIter->advance ()) {
                    myIter->getCurrent (temp);
                    cout << temp << "\n";
                }
            }
            cout << "R vec: \n";
            for (MyDB_PageReaderWriter p: rightBox) {
                MyDB_RecordPtr temp = rightSorted.getEmptyRecord ();
                
                    MyDB_RecordIteratorAltPtr myIter = p.getIteratorAlt ();
                    while (myIter->advance ()) {
                        myIter->getCurrent (temp);
                        cout << temp << "\n";
                    }
                
            }
            
            if(!rightMove && !leftMove){
                cout << "L vec:\n";
                
                for (MyDB_PageReaderWriter p: leftBox) {
                    MyDB_RecordPtr temp = leftSorted.getEmptyRecord ();
                    MyDB_RecordIteratorAltPtr myIter = p.getIteratorAlt ();
                    while (myIter->advance ()) {
                        myIter->getCurrent (temp);
                        cout << temp << "\n";
                    }
                }
                cout << "R vec: \n";
                for (MyDB_PageReaderWriter p: rightBox) {
                    MyDB_RecordPtr temp = rightSorted.getEmptyRecord ();
                    MyDB_RecordIteratorAltPtr myIter = p.getIteratorAlt ();
                        while (myIter->advance ()) {
                            myIter->getCurrent (temp);
                            cout << temp << "\n";
                        }
                    
                }
                MyDB_RecordPtr recl = leftSorted.getEmptyRecord ();
                MyDB_RecordPtr recr = rightSorted.getEmptyRecord ();
                mergeRecs(recl, recr, leftBox, rightBox, output, finalComputations, finalPredicate);
                leftBox.clear();
                rightBox.clear();
                leftMove = true;
                rightMove = true;
            }
        }
    }
//
}

//
//int SortMergeJoin ::nextState(string equality, vector<MyDB_RecordPtr> vec, MyDB_RecordIteratorAltPtr iter, MyDB_RecordPtr rec, func pred){
//    cout << "before push->in vec:\n";
//    for (MyDB_RecordPtr r: vec) {
//        cout << r << "\n";
//    }
//    function <bool ()> f1 = buildRecordComparator(vec.front(), rec, equality);
//    function <bool ()> f2 = buildRecordComparator(rec, vec.front(), equality);
//    if(!f1() && !f2()){
//        cout << "rec = vec.rec\n";
//        int check = checkSingleAcceptance(pred,iter,rec);
//        if(check ==1){
//            return 1;
//        }else if(check==2){
//            return 2;
//        } else {
//            cout << "push...\n";
//            
//            vec.push_back(rec);
//            if(!iter->advance()){
//                return 1;
//            }
//            cout << "in vec:\n";
//            for (MyDB_RecordPtr r: vec) {
//                cout << r << "\n";
//            }
//        }
//    }else{
//        cout << "rec != vec.rec\n";
//        return 3;
//    }
//}

    
int SortMergeJoin ::checkSingleAcceptance(func pred, MyDB_RecordIteratorAltPtr iter, MyDB_RecordPtr rec) {
    cout << "######checkSingleAcceptance:\n";
        iter->getCurrent(rec);
        cout << "record: " << rec << "\n";
        
        // see if it is accepted by the preicate
        if (!pred ()->toBool ()) {
            cout << "not qualified, move to next\n";
            if (!iter->advance()) {
                return 1;//end of the join
            } else {
                iter->getCurrent(rec);
                cout << "now rec: " << rec << "\n";
                return 2;// continue
            }
            
        } else {
            cout << "pass!! stay still\n";

            return 3;//keep on
        }
}
int SortMergeJoin ::checkBothAcceptance(MyDB_RecordIteratorAltPtr iterL, MyDB_RecordPtr recL, MyDB_RecordIteratorAltPtr iterR, MyDB_RecordPtr recR, func pred) {
    cout << "######checkBothAcceptance:\n";
    iterL->getCurrent(recL);
    iterR->getCurrent(recR);
    if (!pred ()->toBool ()) {
        cout << "both not qualified\n";
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
void SortMergeJoin:: mergeRecs (MyDB_RecordPtr leftRec, MyDB_RecordPtr rightRec, vector<MyDB_PageReaderWriter> left, vector<MyDB_PageReaderWriter> right, MyDB_TableReaderWriterPtr output, vector <func> finalComputations, func finalPredicate){
    MyDB_RecordIteratorAltPtr iterL = getIteratorAlt(left);
    MyDB_RecordIteratorAltPtr iterR = getIteratorAlt(right);
    
    while(iterL->advance()){
        iterL->getCurrent(leftRec);
        while(iterR->advance()){
            iterR->getCurrent(rightRec);
            MyDB_RecordPtr outputRec = output->getEmptyRecord ();
            if (finalPredicate ()->toBool ()) {
                
                // run all of the computations
                int i = 0;
                for (auto f : finalComputations) {
                    outputRec->getAtt (i++)->set (f());
                }
                outputRec->recordContentHasChanged ();
                cout << "output: " << outputRec << "\n";
                output->append (outputRec);	
            }
            
        }
    }
}
