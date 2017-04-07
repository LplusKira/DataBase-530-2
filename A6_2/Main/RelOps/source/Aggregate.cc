

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

    //create schema and push all funcs to vector
    MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
    for (auto p : output->getTable ()->getSchema ()->getAtts ()) {
        mySchemaOut->appendAtt (p);

    }
    vector <func> groupingFunc;
    for (auto p : groupings) {
        char *str = (char *) p.c_str ();
        pair <func, MyDB_AttTypePtr> atts = inputRec->compileHelper(str);
        //cout << "att Name: " << p << ", Type: " << atts.second->toString() << "\n";
        groupingFunc.push_back (atts.first);
//        mySchemaOut->appendAtt (make_pair(p, atts.second));
    }
    
    vector <pair<MyDB_AggType, func>> aggFunc;
    for (auto p : aggsToCompute) {
        char *str = (char *) p.second.c_str ();
        pair <func, MyDB_AttTypePtr> atts = inputRec->compileHelper(str);
        pair<MyDB_AggType, func> pair = make_pair(p.first, atts.first);
        aggFunc.push_back (pair);
    }
    // get all data
    cout << "PAGES NUMBER "<< input->getNumPages() << "\n";
    vector <MyDB_PageReaderWriter> allData;
//    for (int i = 0; i < input->getNumPages (); i++) {
//        MyDB_PageReaderWriter temp = input->getPinned (i);
//        if (temp.getType () == MyDB_PageType :: RegularPage){
//            allData.push_back (temp);
//        }
//    }
    cout << "finished all data\n";
    func pred = inputRec->compileComputation (selectionPredicate);
    
    
    MyDB_RecordPtr groupedRec = make_shared <MyDB_Record> (mySchemaOut);
    MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt (0,input->getNumPages());
    
    int currPagecount = 0;
    int recNum = 0;
    cout << "start iter\n";

    while (myIter->advance ()) {
        
        // hash the current record
        myIter->getCurrent (inputRec);
//        cout << "INPUT REC "<< inputRec << "\n";
        recNum++;
        // see if it is accepted by the preicate
        if (!pred ()->toBool ()) {
            continue;
        }
//        cout << inputRec << "\n";
        // compute its hash
        size_t hashVal = 0;
        for (auto f : groupingFunc) {
            hashVal ^= f ()->hash ();
//            cout << "hashVal: " << hashVal << "\n";
        }
        
        // see if it is in the hash table
        if (myHash.count (hashVal) == 0) {
//            cout << "not in hash...........\n";
            countMap[hashVal] = 1;
            //not in hash, add (newkey, newval)
            int i = 0;
            for (auto f : groupingFunc) {
                groupedRec->getAtt (i++)->set (f());
            }
            for (auto p : aggFunc) {
                if (p.first == cnt) {
                    MyDB_IntAttValPtr att = make_shared<MyDB_IntAttVal>();
                    att->set(countMap[hashVal]);
                    groupedRec->getAtt (i)->set (att);
                    i++;
                }
                if(p.first==sum){
                    groupedRec->getAtt (i)->set (p.second());
                    cout << "bad access: " << groupedRec << "\n";
                    sumMap[hashVal]=groupedRec->getAtt(i)->toDouble();
                    i++;
                }
                if(p.first==avg){
                    groupedRec->getAtt (i)->set (p.second());
                    sumMap[hashVal]=(groupedRec->getAtt(i)->toDouble())*countMap[hashVal];
                    i=i+1;
                }
                
            }
            // the record's content has changed because it
            // is now a composite of two records whose content
            // has changed via a read... we have to tell it this,
            // or else the record's internal buffer may cause it
            // to write old values
            groupedRec->recordContentHasChanged ();
            MyDB_PageReaderWriter toPage = output->getPinned(currPagecount);
            void* loc = toPage.appendAndReturnLocation(groupedRec);
//            MyDB_RecordPtr temp = output->getEmptyRecord ();
//            MyDB_RecordIteratorAltPtr myIter2 = toPage.getIteratorAlt ();
//            
//            cout << "count the records.";
//            
//            while (myIter2->advance ()) {
//                myIter2->getCurrent (temp);
//                cout << temp << "\n";
//            }
//            cout << "end of count\n";
            if (loc == nullptr) {
//                cout << currPagecount << " page fulls\n";
                currPagecount++;
                MyDB_PageReaderWriter toPages = output->getPinned(currPagecount);
                myHash [hashVal] = toPages.appendAndReturnLocation(groupedRec);
            } else {
                myHash [hashVal] = loc;
            }
            
        } else {
//            cout << "in hash................\n";
            //In hash? val+=newval
            //If you need to update the aggregate, you can use fromBinary () on the record to reconstitute it, then change the value as needed (using a pre-compiled func object) and then use toBinary () to write it back again.
            groupedRec->fromBinary (myHash[hashVal]);
//            cout << "hash val: " << hashVal << "\n";
            countMap[hashVal] += 1;
            int i = groupingFunc.size();
//            cout << "agg att starts index " << i << "\n";
            for (auto p : aggFunc) {
                if (p.first == sum) {
                    cout << "SUM\n";
                    groupedRec->getAtt(i)->set(p.second());
                    cout << "sum groupedRec" <<groupedRec->getAtt(i)->toDouble()<<"\n";
//                    sum += groupedRec->getAtt(i)->toDouble();
                    sumMap[hashVal] += groupedRec->getAtt(i)->toDouble();
                    cout << "sum hash" <<sumMap[hashVal] <<"\n";
                    MyDB_IntAttValPtr att = make_shared<MyDB_IntAttVal>();
                    att->set(sumMap[hashVal]);
                    //                    cout << "pre the val: " << groupedRec->getAtt(i)->toInt() << "\n";
                    groupedRec->getAtt (i)->set (att);
                    i++;
                  
                } else if (p.first == avg) {
//                    cout << "AVG\n";
                    groupedRec->getAtt(i)->set(p.second());
                    sumMap[hashVal]=(groupedRec->getAtt(i)->toDouble())*countMap[hashVal];
                    i++;
                } else if (p.first == cnt) {
//                    cout << "COUNT\n";
                    MyDB_IntAttValPtr att = make_shared<MyDB_IntAttVal>();
                    att->set(countMap[hashVal]);
//                    cout << "pre the val: " << groupedRec->getAtt(i)->toInt() << "\n";
                    groupedRec->getAtt (i)->set (att);
//                    cout << "now the val: " << groupedRec->getAtt(i)->toInt() << "\n";
                    i++;
                }
            }
//            cout << "count: " << countMap[hashVal] << "\n";
//            cout << "sum: " << sumMap[hashVal] << "\n";
            groupedRec->recordContentHasChanged ();
            groupedRec->toBinary(myHash[hashVal]);
//            cout << "write back\n";
        }
        
        
    }
}
