

#include "MyDB_TableReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "Aggregate.h"
#include "MyDB_Record.h"
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>

#include "MyDB_Schema.h"
#include <iostream>


// This class encapulates a simple, hash-based aggregation + group by.  It does not
// need to work when there is not enough space in the buffer manager to store all of
// the groups.
using namespace std;

Aggregate::Aggregate (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
		vector <pair <MyDB_AggType, string>> aggsToCompute,
           vector <string> groupings, string selectionPredicate) {
    this->input = input;
    this->output = output;
    this->aggsToCompute = aggsToCompute;
    this->groupings = groupings;
    this->selectionPredicate = selectionPredicate;
}
	
	// execute the aggregation
void Aggregate::run () {
    // this is the hash map we'll use to look up data... the key is the hashed value
    // of all of the records' join keys, and the value is a list of pointers were all
    // of the records with that hsah value are located
    unordered_map <size_t, void *> myHash;
    unordered_map <size_t, double> sumMap;
    unordered_map <size_t, int> countMap;
    
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();

    //create schema ?????????
    MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
    for (auto p : groupings) {
        char *str = (char *) p.c_str ();
        pair <func, MyDB_AttTypePtr> atts = inputRec->compileHelper(str);
        cout << "att Name: " << p << ", Type: " << atts.second->toString() << "\n";
        mySchemaOut->appendAtt (make_pair(p, atts.second));
    }
    for (auto p : aggsToCompute) {
        char *str = (char *) p.second.c_str ();
        pair <func, MyDB_AttTypePtr> atts = inputRec->compileHelper(str);
        if (p.first == sum) {
           mySchemaOut->appendAtt (make_pair("sum", atts.second));
           cout << "att Name: sum, Type: " << atts.second->toString() << "\n";
        } else if (p.first == avg) {
           mySchemaOut->appendAtt (make_pair("avg", atts.second));
            cout << "att Name: avg, Type: " << atts.second->toString() << "\n";
        } else if (p.first == cnt) {
            mySchemaOut->appendAtt (make_pair("count", atts.second));
            cout << "att Name: count, Type: " << atts.second->toString() << "\n";
        }
    }
    // get all data
    vector <MyDB_PageReaderWriter> allData;
    for (int i = 0; i < input->getNumPages (); i++) {
        MyDB_PageReaderWriter temp = input->getPinned (i);
        if (temp.getType () == MyDB_PageType :: RegularPage){
            allData.push_back (input->getPinned (i));
        }
    }
    func pred = inputRec->compileComputation (selectionPredicate);
    
    // get all grouping func
    vector <func> groupingFunc;
    for (auto p : groupings) {
        groupingFunc.push_back (inputRec->compileComputation (p));
    }
    vector <std::pair<MyDB_AttType, func>> aggFunc;
    for (auto p : aggsToCompute) {
        pair<MyDB_AttType, func> pair = make_pair(p.first, inputRec->compileComputation (p.second));
        aggFunc.push_back (pair);
    }
    MyDB_RecordPtr groupedRec = make_shared <MyDB_Record> (mySchemaOut);
    MyDB_RecordIteratorAltPtr myIter = getIteratorAlt (allData);
    int outpageNum = output->getNumPages();
    int currPagecount = 0;
    MyDB_PageReaderWriter toPage = output->getPinned(currPagecount);
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
            countMap[hashVal] = 1;
            //not in hash, add (newkey, newval)
            int i = 0;
            for (auto f : groupingFunc) {
                groupedRec->getAtt (i++)->set (f());
            }
            for (auto p : aggFunc) {
                sumMap[hashVal] = groupedRec->getAtt(i)->toDouble();
                groupedRec->getAtt (i++)->set (p.second());
                
            }
            // the record's content has changed because it
            // is now a composite of two records whose content
            // has changed via a read... we have to tell it this,
            // or else the record's internal buffer may cause it
            // to write old values
            groupedRec->recordContentHasChanged ();
            void* loc = toPage.appendAndReturnLocation(groupedRec);
            if (loc == nullptr) {
                currPagecount++;
                toPage = output->getPinned(currPagecount);
                myHash [hashVal] = toPage.appendAndReturnLocation(groupedRec);
            } else {
                myHash [hashVal] = loc;
            }
            
        } else {
            //In hash? val+=newval
            //If you need to update the aggregate, you can use fromBinary () on the record to reconstitute it, then change the value as needed (using a pre-compiled func object) and then use toBinary () to write it back again.
            void* preLoc = groupedRec->fromBinary (myHash[hashVal]);
            countMap[hashVal] += 1;
            int i = groupingFunc.size();
            for (auto p : aggFunc) {
                sumMap[hashVal] += groupedRec->getAtt(i)->toDouble();
                if (p.first == sum) {
                    double sum = sumMap[hashVal];
                    if (groupedRec->getAtt (i)->promotableToInt()) {
                        MyDB_IntAttValPtr att = make_shared<MyDB_IntAttVal>();
                        att->set((int)sum);
                        groupedRec->getAtt (i)->set (att);
                        i++;
                    } else {
                        MyDB_DoubleAttValPtr att = make_shared<MyDB_DoubleAttVal>();
                        att->set(sum);
                        groupedRec->getAtt (i)->set (att);
                        i++;
                    }
                } else if (p.first == avg) {
                    double average = sumMap[hashVal]/countMap[hashVal];
                    MyDB_DoubleAttValPtr att = make_shared<MyDB_DoubleAttVal>();
                    att->set(average);
                    groupedRec->getAtt (i)->set (att);
                    i++;
                } else if (p.first == cnt) {
                    MyDB_IntAttValPtr att = make_shared<MyDB_IntAttVal>();
                    att->set(countMap[hashVal]);
                    groupedRec->getAtt (i)->set (att);
                    i++;
                }
            }
            groupedRec->toBinary(preLoc);
        }
        
        
    }
    
    
}

