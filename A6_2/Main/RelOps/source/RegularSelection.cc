

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
}
