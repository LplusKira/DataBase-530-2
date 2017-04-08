
#ifndef BPLUS_TEST_H
#define BPLUS_TEST_H

#include "MyDB_AttType.h"
#include "MyDB_BufferManager.h"
#include "MyDB_Catalog.h"
#include "MyDB_Page.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_Record.h"
#include "MyDB_Table.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_Schema.h"
#include "QUnit.h"
#include "Sorting.h"
#include "ScanJoin.h"
#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"
#include <iostream>
#include <vector>
#include <utility>

using namespace std;

int main () {
    
    QUnit::UnitTest qunit(cerr, QUnit::verbose);
    
    // create a catalog
    MyDB_CatalogPtr myCatalog = make_shared <MyDB_Catalog> ("catFile");
    
    // now make a schema
    MyDB_SchemaPtr mySchemaL = make_shared <MyDB_Schema> ();
    mySchemaL->appendAtt (make_pair ("l_suppkey", make_shared <MyDB_IntAttType> ()));
    mySchemaL->appendAtt (make_pair ("l_name", make_shared <MyDB_StringAttType> ()));
    mySchemaL->appendAtt (make_pair ("l_address", make_shared <MyDB_StringAttType> ()));
    mySchemaL->appendAtt (make_pair ("l_nationkey", make_shared <MyDB_IntAttType> ()));
    mySchemaL->appendAtt (make_pair ("l_phone", make_shared <MyDB_StringAttType> ()));
    mySchemaL->appendAtt (make_pair ("l_acctbal", make_shared <MyDB_DoubleAttType> ()));
    mySchemaL->appendAtt (make_pair ("l_comment", make_shared <MyDB_StringAttType> ()));
    
    // and a right schema
    MyDB_SchemaPtr mySchemaR = make_shared <MyDB_Schema> ();
    mySchemaR->appendAtt (make_pair ("r_suppkey", make_shared <MyDB_IntAttType> ()));
    mySchemaR->appendAtt (make_pair ("r_name", make_shared <MyDB_StringAttType> ()));
    mySchemaR->appendAtt (make_pair ("r_address", make_shared <MyDB_StringAttType> ()));
    mySchemaR->appendAtt (make_pair ("r_nationkey", make_shared <MyDB_IntAttType> ()));
    mySchemaR->appendAtt (make_pair ("r_phone", make_shared <MyDB_StringAttType> ()));
    mySchemaR->appendAtt (make_pair ("r_acctbal", make_shared <MyDB_DoubleAttType> ()));
    mySchemaR->appendAtt (make_pair ("r_comment", make_shared <MyDB_StringAttType> ()));
    
    // use the schema to create a table
    MyDB_TablePtr myTableLeft = make_shared <MyDB_Table> ("supplierLeft", "supplierLeft.bin", mySchemaL);
    MyDB_TablePtr myTableRight = make_shared <MyDB_Table> ("supplierRight", "supplierRight.bin", mySchemaR);
    
    // get the tables
    MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1310720, 256, "tempFile");
    MyDB_TableReaderWriterPtr supplierTableL = make_shared <MyDB_TableReaderWriter> (myTableLeft, myMgr);
    MyDB_TableReaderWriterPtr supplierTableRNoBPlus = make_shared <MyDB_TableReaderWriter> (myTableRight, myMgr);
    
    // load up from a text file
    cout << "loading left table.\n";
    // supplierTableL->loadFromTextFile ("smallSupplier.tbl");
   	supplierTableL->loadFromTextFile ("/Users/xiajunru/Code/DataBase-530-2/A6_2/Build/supplier.tbl");
    
    cout << "loading right table.\n";
    supplierTableRNoBPlus->loadFromTextFile ("/Users/xiajunru/Code/DataBase-530-2/A6_2/Build/supplierBig.tbl");
   	// supplierTableRNoBPlus->loadFromTextFile ("supplierBig.tbl");
    
    
    
    
    {
        
        // get the output schema and table
        MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
        mySchemaOut->appendAtt (make_pair ("l_name", make_shared <MyDB_StringAttType> ()));
        mySchemaOut->appendAtt (make_pair ("combined_comment", make_shared <MyDB_StringAttType> ()));
        MyDB_TablePtr myTableOut = make_shared <MyDB_Table> ("supplierOut", "supplierOut.bin", mySchemaOut);
        MyDB_TableReaderWriterPtr supplierTableOut = make_shared <MyDB_TableReaderWriter> (myTableOut, myMgr);
        
        // This basically runs:
        //
        // SELECT supplierLeft.l_name, supplierLeft.l_comment + " " + supplierRight.r_comment
        // FROM supplierLeft, supplierRight
        // WHERE (supplierLeft.l_nationkey = 4 OR
        //        supplierLeft.l_nationkey = 3) AND
        //       (supplierRight.r_nationkey = 3) AND
        //       (supplierLeft.l_suppkey = supplierRight.r_suppkey) AND
        //       (supplierLeft.l_name = supplierRight.r_name)
        //
        // It does this by hashing the smaller table (supplierLeft) on
        // supplierLeft.l_suppkey and supplierLeft.l_name.  It then scans
        // supplierRight, probing the hash table for matches
        
        vector <pair <string, string>> hashAtts;
        hashAtts.push_back (make_pair (string ("[l_suppkey]"), string ("[r_suppkey]")));
        hashAtts.push_back (make_pair (string ("[l_name]"), string ("[r_name]")));
        
        vector <string> projections;
        projections.push_back ("[l_name]");
        projections.push_back ("+ (+ ([l_comment], string[ ]), [r_comment])");
        
        cout << "Do you want to run a:\n";
        cout << "\t1. Sort merge join.\n";
        cout << "\t2. Scan join.\n";
        cout << "Enter 1 or 2:\n";
        int res;
        cin >> res;
        
        if (res == 2) {
            ScanJoin myOp (supplierTableL, supplierTableRNoBPlus, supplierTableOut,
                           "&& ( == ([l_suppkey], [r_suppkey]), == ([l_name], [r_name]))", projections, hashAtts,
                           "|| ( == ([l_nationkey], int[3]), == ([l_nationkey], int[4]))",
                           "== ([r_nationkey], int[3])");
            cout << "running join\n";
            myOp.run ();
        } else if (res == 1) {
            SortMergeJoin myOp (supplierTableL, supplierTableRNoBPlus, supplierTableOut,
                                "&& ( == ([l_suppkey], [r_suppkey]), == ([l_name], [r_name]))", projections,
                                make_pair (string ("[l_suppkey]"), string ("[r_suppkey]")),
                                "|| ( == ([l_nationkey], int[3]), == ([l_nationkey], int[4]))",
                                "== ([r_nationkey], int[3])");
            cout << "running join\n";
            myOp.run ();
        } else {
            cout << "I said 1 or 2!!!\n";
            return 3;
        }
        
        // now, we count the total number of records with each nation name
        vector <pair <MyDB_AggType, string>> aggsToCompute;
        aggsToCompute.push_back (make_pair (MyDB_AggType :: cnt, "int[0]"));
        
        vector <string> groupings;
        groupings.push_back ("[l_name]");
        
        MyDB_SchemaPtr mySchemaOutAgain  = make_shared <MyDB_Schema> ();
        mySchemaOutAgain->appendAtt (make_pair ("l_name", make_shared <MyDB_StringAttType> ()));
        mySchemaOutAgain->appendAtt (make_pair ("mycnt", make_shared <MyDB_IntAttType> ()));
        MyDB_TablePtr aggTable = make_shared <MyDB_Table> ("aggOut", "aggOut.bin", mySchemaOutAgain);
        MyDB_TableReaderWriterPtr aggTableOut = make_shared <MyDB_TableReaderWriter> (aggTable, myMgr);
        
        Aggregate myOpAgain (supplierTableOut, aggTableOut, aggsToCompute, groupings,
                             "&& ( > ([l_name], string[Supplier#000002243]), < ([l_name], string[Supplier#000002303]))");
        cout << "running aggregate\n";
        myOpAgain.run ();
        
        MyDB_RecordPtr temp = aggTableOut->getEmptyRecord ();
        MyDB_RecordIteratorAltPtr myIter = aggTableOut->getIteratorAlt ();
        
        cout << "\nThe output should be, in some order:\n";
        cout << "Supplier#000002245|32|\n";
        cout << "Supplier#000002264|32|\n";
        cout << "Supplier#000002265|32|\n";
        cout << "Supplier#000002272|32|\n";
        cout << "Supplier#000002282|32|\n";
        cout << "\nHere goes:\n";
        while (myIter->advance ()) {
            myIter->getCurrent (temp);
            cout << temp << "\n";
        }
    }
    
    //MyDB_BPlusTreeReaderWriterPtr supplierTableR = make_shared <MyDB_BPlusTreeReaderWriter> ("r_address", myTableRight, myMgr);
//    MyDB_TablePtr myTableRightNoBPlus = make_shared <MyDB_Table> ("supplierRightNoBPlus", "supplierRightNoBPlus.bin", mySchemaR);
    
  //  cout << "loading right into B+-Tree indexed on r_address.\n";
    //supplierTableR->loadFromTextFile ("/Users/kejunliu/Documents/DataBase-530-2/A6_2/Build/supplierBig.tbl");
    

}

#endif
