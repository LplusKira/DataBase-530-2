

#include "RegularSelection.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"

using namespace std;

RegularSelection :: RegularSelection (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                                      string selectionPredicate, vector <string> projections){
    input = input;
    output = output;
    projections = projections;
    selectionPredicate = selectionPredicate;

}

// execute the selection operation
void RegularSelection :: run (){
    vector <MyDB_PageReaderWriter> allData;
    for (int i = 0; i < input->getNumPages (); i++) {
        MyDB_PageReaderWriter temp = input->getPinned (i);
        if (temp.getType () == MyDB_PageType :: RegularPage){
            allData.push_back (leftTable->getPinned (i));
        }
    }
    func leftPred = leftInputRec->compileComputation (leftSelectionPredicate);
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();
    MyDB_RecordIteratorAltPtr myIter = getIteratorAlt (allData);
    
    while (myIter->advance ()) {
        
        // hash the current record
        myIter->getCurrent (inputRec);
        
        // see if it is accepted by the preicate
        if (!leftPred ()->toBool ()) {
            continue;
        }
        
        // compute its hash
        size_t hashVal = 0;
        for (auto f : leftEqualities) {
            hashVal ^= f ()->hash ();
        }
        
        // see if it is in the hash table
        myHash [hashVal].push_back (myIter->getCurrentPointer ());
    }
    vector <func> finalComputations;
    for (string s : projections) {
        finalComputations.push_back (combinedRec->compileComputation (s));
    }
    // this is the output record
    MyDB_RecordPtr outputRec = output->getEmptyRecord ();
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
