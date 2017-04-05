

#include <stdio.h>

#include "MyDB_BPlusTreeReaderWriter"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "BPlusSelection.h"
#include "MyDB_Record.h"
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>



using namespace std;

BPlusSelection :: BPlusSelection (MyDB_BPlusTreeReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                MyDB_AttValPtr low, MyDB_AttValPtr high,
                                  string selectionPredicate, vector <string> projections){
    this->input  = input;
    this->output = output;
    this->low = low;
    this->high = high;
    this->selectionPredicate = selectionPredicate;
    this->projections = projections;
}

// execute the selection operation
void BPlusSelection :: run (){
    
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();
    func pred = inputRec->compileComputation (selectionPredicate);
    vector <func> finalComputations;
    for (string s : projections) {
        finalComputations.push_back (inputRec->compileComputation (s));
    }
    
    MyDB_RecordIteratorAltPtr myIter = input->getRangeIteratorAlt (low,high);
    // this is the output record
    MyDB_RecordPtr outputRec = output->getEmptyRecord ();
    
    while (myIter->advance ()) {
        
        // hash the current record
        myIter->getCurrent (inputRec);
        
        // see if it is accepted by the preicate
        if (!pred ()->toBool ()) {
            continue;
        }
        // run all of the computations
        int i = 0;
        for (auto f : finalComputations) {
            outputRec->getAtt (i++)->set (f());
        }
        
        // the record's content has changed because it
        // is now a composite of two records whose content
        // has changed via a read... we have to tell it this,
        // or else the record's internal buffer may cause it
        // to write old values
        outputRec->recordContentHasChanged ();
        output->append (outputRec);
    }

}
