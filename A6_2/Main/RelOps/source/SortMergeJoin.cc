

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
    l;trd
}
