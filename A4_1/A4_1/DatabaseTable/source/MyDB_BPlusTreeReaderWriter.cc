
#ifndef BPLUS_C
#define BPLUS_C
#include <stack>
#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_RunQueueIteratorAlt.h"
#include "IteratorComparator.h"
#include "MyDB_PageListIteratorAlt.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {
    
    cout << "MyDB_BPlusTreeReaderWriter()\n";
    
	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
    whichAttIsOrdering = res.first;
    
    initialize();
}

// gets an instance of an alternate iterator over the table... this is an
// iterator that has the alternate getCurrent ()/advance () interface... returned records must be sorted
// return all records with a key value in the range [low, high], inclusive
MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
    MyDB_INRecordPtr head = getINRecord();
    head->setKey(low);
    MyDB_INRecordPtr tail = getINRecord();
    tail->setKey(high);
    
    if (compareTwoRecords(low, high)) {
        cout << "low < high\n";
        vector<int> result;
        vector<MyDB_PageReaderWriter> retPages;
        queue<int> pages;
        pages.push(getTable()->getRootLocation());
        
        while(!pages.empty()) {
            int curPage = pages.front();
            pages.pop();
            result.clear();
            if (discoverPages(curPage, result, low, high)) {
                cout << " internal, continue discover \n";
                for (int page : result) {
                    pages.push(page);
                }
            } else {
                cout << "leaf, add these pages to result pages \n";
                for (int page: result) {
                    retPages.push_back(*getByID(page));
                }
            }
        }
        MyDB_RecordIteratorAltPtr iter = make_shared <MyDB_PageListIteratorAlt> (retPages);
        return iter;
    } else {
        
        cout << "low >= high\n";
        return nullptr;
    }

}

// gets an instance of an alternate iterator over the table... this is an
// iterator that has the alternate getCurrent ()/advance () interface
// return all records with a key value in the range [low, high], inclusive
MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
    MyDB_INRecordPtr head = getINRecord();
    head->setKey(low);
    MyDB_INRecordPtr tail = getINRecord();
    tail->setKey(high);
    cout << "low key: " << low->toInt() << "\n";
    cout << "high key: " << high->toInt() << "\n";
    
    if (!compareTwoRecords(head->getKey(), tail->getKey())) {
        cout << "low <= high\n";
        vector<int> result;
        vector<MyDB_PageReaderWriter> retPages;
        queue<int> pages;
        cout << "push root: " << getTable()->getRootLocation() << "\n";
        pages.push(getTable()->getRootLocation());
        
        //get first
        MyDB_INRecordPtr firstPage = getINRecord();
        MyDB_RecordIteratorPtr find1stIter = getByID(getTable()->getRootLocation())->getIterator(firstPage);
        if (find1stIter ->hasNext()) {
            find1stIter->getNext();
        }
        if (compareTwoRecords(low, firstPage->getKey())) {
            // if has page 1, do not push it again
            cout << "push 1st page\n";
            retPages.push_back(*getByID(1));
        }
        
        while(!pages.empty()) {
            int curPage = pages.front();
            if (getByID(curPage)->getType() == RegularPage) {
                cout <<  curPage << " is a leaf page\n";
                retPages.push_back(*getByID(curPage));
                pages.pop();
                result.clear();
            } else {
                cout << "curPage: " << curPage << "\n";
                pages.pop();
                result.clear();
                discoverPages(curPage, result, low, high);
                for (int page : result) {
                    pages.push(page);
                }
            }
        }
        MyDB_RecordIteratorAltPtr iter = make_shared <MyDB_PageListIteratorAlt> (retPages);
        return iter;
    } else {
        
        cout << "low > high\n";
        return nullptr;
    }
    
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <int> &list, MyDB_AttValPtr low, MyDB_AttValPtr high) {
    
    cout << "*************discover page now***********\n";
    MyDB_INRecordPtr head=make_shared<MyDB_INRecord>(low);
    MyDB_INRecordPtr tail=make_shared<MyDB_INRecord>(high);
    cout << "head: " << low->toInt() << "\n";
    cout << "tail: " << high->toInt() << "\n";

    MyDB_INRecordPtr current2 = this->getINRecord();
    MyDB_RecordIteratorPtr myIter2 = getByID(whichPage)->getIterator(current2);
    cout << "~~~~~in page " << whichPage << "~~~~~~~\n";
    while (myIter2 -> hasNext()) {
        myIter2->getNext();
        cout << "->find path: record starts with " << current2->getPtr() << ", key:" << current2->getKey()->toInt()<< "\n";
    }
    
    
    MyDB_INRecordPtr cur = this->getINRecord();
    MyDB_RecordIteratorPtr myIter = getByID(whichPage)->getIterator(cur);
    int prePtr = -1;
    if (myIter ->hasNext()) {
        myIter->getNext();
    }
    // find lowest page
    // while cur is smaller than head, go ahead
    cout << "curr:" << cur->getKey()->toInt() << " < head:" << head->getKey()->toInt() << "\n";
    while (true) {
        if (compareTwoRecords( cur->getKey(), low)) {
            cout << "curr:" << cur->getKey()->toInt() << " < head:" << head->getKey()->toInt() << "\n";
            if(myIter->hasNext()){
                prePtr=cur->getPtr();
                cout << "set prePtr: " << prePtr << "\n";
                myIter->getNext();
            } else {
                break;
            }
        } else {
            cout << "curr >= head\n";
            break;
        }
        
    }
    // find the first inRecord that is larger or equal to head
    if (prePtr == -1) {
        cout << "no one smaller than low\n";
    } else {
        cout << "found 1st and pushed\n";
        list.push_back(prePtr);
    }
    
    // find upper page
    while(!compareTwoRecords(high, cur->getKey())){
        cout << "cur >= tail\n";
        cout << "curr: " << cur->getKey()->toInt() << "\n";
        list.push_back(cur->getPtr());
        if(myIter->hasNext()){
            myIter->getNext();
        }else{
            break;
        }
    }
    cout << "\n\ncheck list here:\n\n";
    for (int check: list) {
        cout << check << " , ";
    }
    cout << "\n";
//    if(getByID(list.back())->getType()== RegularPage){
//        cout << "page <" << list.back() << "> is a leaf page\n";
//        return false;
//    }
    return true;
}
void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr record) {
    //using stack
    cout << "append\n";
//    cout << "#####" << record << "\n";
    // when run loadFromText, the root and leaf are cleared, so we need to create again
    if (forMe->lastPage() == 0) {
        initialize();
    }

    
    std::stack<int> stack;
    stack.push(getTable()->getRootLocation());
//            MyDB_INRecordPtr current2 = this->getINRecord();
//            MyDB_RecordIteratorPtr myIter2 = getByID(stack.top())->getIterator(current2);
//            while (myIter2 -> hasNext()) {
//                myIter2->getNext();
//                cout << "->find path: record starts with " << current2->getPtr() << ", key:" << current2->getKey()->toInt()<< "\n";
//            }

    //find the page and push all path into stack
    while (getByID(stack.top())->getType() != RegularPage) {
        
        MyDB_INRecordPtr current = this->getINRecord();
        MyDB_RecordIteratorPtr fastIter = getByID(stack.top())->getIterator(current);
        int prePtr  = 1;
//        if (myIter->hasNext() && buildComparator(current, record)) {
//            cout << "have a smaller record\n";
//            append(getByID(1), record);
//            break;
//        }
        
//        if (buildComparator(current, record)) {
//            cout << "<\n";
//        } else {
//            cout << ">\n";
//        }
        //jump to the 1st record first
        
        if (fastIter ->hasNext()) {
            fastIter->getNext();
        }
        while (fastIter->hasNext()) {
//            cout << "before current ptr " << current->getPtr() << ", key: " << current->getKey()->toInt() << "\n";
            // larger or equal to
            if (!buildComparator(current, record)) {
//                cout << "current >= insert\n";
                
                break;
            } else {
                int temp = current->getPtr();
                prePtr = temp;
//                cout << "current < insert\n";
                fastIter->getNext();
            }
//            cout << "after current ptr " << current->getPtr() << ", key: " << current->getKey()->toInt() << "\n";
        }
        
//        cout << "current ptr " << current->getPtr() << ", key: " << current->getKey()->toInt() << "\n";
//        cout << "pre ptr " << prePtr << "\n";
//        cout << "record key: " << record->getAtt(whichAttIsOrdering)->toInt() << "\n";
        int index = prePtr;
//        cout << "index" << index << "\n";
        //cout  << current->getAtt(0)->toInt() << "\n";
//        cout << "current points to: " << current->getPtr() << "\n";
        //cout  << record->getAtt(0)->toInt() << "\n";
//        cout << "push: " << index << "\n";
        stack.push(index);
    }
    
    // start to append
    MyDB_RecordPtr new_record = append(stack.top(), record);
    
    while ( new_record != NULL) {
        
        if (!stack.empty()) {
            stack.pop();
        }
        if (!stack.empty()) {
            
            new_record = append(stack.top(), new_record);
            // after append, if is internal page, inplace sort it
            sortPage(getByID(stack.top()));
        } else {
            //create new root
            cout << "\n\n`````create new root \n\n";
//            cout << " before, root is " << getTable()->getRootLocation() << "\n";
            forMe->setLastPage(forMe->lastPage() + 1);
            MyDB_PageReaderWriterPtr newRoot = make_shared <MyDB_PageReaderWriter> (*this, forMe->lastPage ());
            newRoot->clear ();
            newRoot->setType(DirectoryPage);
            
            //get 1st record in old root
            int oldRoot = getTable ()->getRootLocation ();
            MyDB_PageReaderWriterPtr leftPage = getByID(oldRoot);
            MyDB_INRecordPtr temp = this->getINRecord();
            MyDB_RecordIteratorPtr myIter = leftPage->getIterator(temp);
            if (myIter->hasNext()) {
                myIter->getNext();
            }
            
            MyDB_INRecordPtr record1 = getINRecord();
            record1->setKey(temp->getKey());
            record1->setPtr(oldRoot);
            
            newRoot->append(record1);
            newRoot->append(new_record);
            
            getTable()->setRootLocation(forMe->lastPage());
            root = newRoot;
            
            break;
        }
        
    }
    if (!stack.empty()) {
        sortPage(getByID(stack.top()));
    }
        //printTree();
}


MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter, MyDB_RecordPtr) {
	return nullptr;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichpage, MyDB_RecordPtr record) {
    MyDB_PageReaderWriterPtr page = getByID(whichpage);
    cout << "\n\ntry to append to page : " << whichpage << "\n";
    cout << record << "\n\n\n";
    
    if(page->append(record)){
        cout << "easy append\n";
        return nullptr;
    }else{
        //harder case
        cout << "\n\nhard append\n";
        
        //1. sort
        if (page->getType() == RegularPage) {
            cout << "sort for leaf page\n";
            MyDB_RecordPtr lhr = this->getEmptyRecord();
            MyDB_RecordPtr rhr = this->getEmptyRecord();
            function<bool()> myComp=buildComparator(lhr, rhr);
            page->sortInPlace(myComp, lhr, rhr);

        } else {
            cout << "sort for internal page\n";
            MyDB_INRecordPtr lhr = this->getINRecord();
            MyDB_INRecordPtr rhr = this->getINRecord();
            function<bool()> myComp=buildComparator(lhr, rhr);
            page->sortInPlace(myComp, lhr, rhr);

        }
        //2. find median
        int size = (int)page->getPageSize();
        //1. create new page
        forMe->setLastPage(forMe->lastPage()+1);
        MyDB_PageReaderWriterPtr newPage = make_shared <MyDB_PageReaderWriter> (*this, forMe->lastPage());
        MyDB_PageType page_Type = page->getType();
        newPage->clear();
        newPage->setType(page_Type);
        //2. copy upper half to new page
        MyDB_RecordIteratorAltPtr myIter = page->getIteratorAlt();
        std::vector<MyDB_RecordPtr> recs;
        int bytesConsumed = sizeof (size_t) * 2;
        cout << "\n\noriginal records\n:";
        while (bytesConsumed < size) {
            //cout << bytesConsumed << "byyyyyy\n";
            if(myIter->advance()){
                if (page->getType() == RegularPage) {
                    MyDB_RecordPtr temp = this->getEmptyRecord();
                    myIter->getCurrent(temp);
                    cout << temp << "\n";
                    recs.push_back(temp);
                    int consume=temp->getBinarySize();
                    bytesConsumed += consume;
                } else {
                    MyDB_INRecordPtr temp = this->getINRecord();
                    myIter->getCurrent(temp);
                    cout << temp << "\n";
                    recs.push_back(temp);
                    int consume=temp->getBinarySize();
                    bytesConsumed += consume;
                }
                
            }else{
                break;
            }
        }
        cout << "\n\n";
        cout << "vector records\n:";
        for(int i=0;i<recs.size();i++){
            cout << recs[i]<<"gggggggg\n";
        }
        cout << "\n\n";
        ///////////
        int medium=recs.size()/2;
        MyDB_PageType pageType = page->getType();
        page->clear();
        page->setType(pageType);
        MyDB_RecordPtr median=recs.at(medium);
        cout << "lower half:\n";
        for(int i=0;i<recs.size()/2;i++){
            cout << recs.at(i)<< "\n";
            if(page->append(recs.at(i))){
                cout << "success\n";
            }else{
                cout << "ummmm ....... cannot append to lower part\n";
            }
        }
        cout << "\n\n";
        cout << "upper half\n";
        for(int i=recs.size()/2;i<recs.size();i++){
            cout << recs.at(i)<< "\n";
            if(newPage->append(recs.at(i))){
                cout << "success\n";
            }else{
                cout << "ummmm ....... cannot append to upper part\n";
            }
        }
        cout << "\n\n";
        
//        cout << "record: " << record << "\n";
//        cout << "median:" << median << "\n";
        //cout << buildComparator(record, median) << "\n";
        if(!buildComparator(record, median)){
            if(page->append(record)){
                cout << "append to lower part\n";
            }else{
                cout << "ummmm  cannot append to lower part\n";
            }
        }else{
            cout << record << "record\n";
            if(newPage->append(record)){
                cout << "append to upper part\n";
            }else{
                cout << "ummmm  cannot append to upper part\n";
            }
        }
        
        //returns a new MyDB_INRecordPtr object that points to an appropriate internal node record.
        MyDB_INRecordPtr newInRec = getINRecord();
        if (page->getType() == RegularPage) {
            newInRec->setKey(getKey(median));
        } else {
            newInRec->setKey(getKey(median));
        }
        newInRec->setPtr(forMe->lastPage());
        return newInRec;
    }
}
MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
    std::stack<MyDB_PageReaderWriterPtr> stack;
    stack.push(this->root);
    int count = 0; // # of records
    while (!stack.empty())
    {
        MyDB_PageReaderWriterPtr current = stack.top();
        
        if (current->getType() == DirectoryPage) {
            //cout << "this is an internal page.\n";
            MyDB_INRecordPtr temp = this->getINRecord();
            MyDB_RecordIteratorPtr myIter = current->getIterator(temp);
            while (myIter->hasNext()) {
                int index = temp->getPtr();
                stack.push(getByID(index));
                myIter->getNext();
            }
            //cout << "# record: " << count << "\n";
            stack.pop();
        } else {
            cout << "this is a leaf page.\n";
            MyDB_RecordPtr temp = this->getEmptyRecord();
            MyDB_RecordIteratorPtr myIter = current->getIterator(temp);
            while (myIter->hasNext()) {
                count++;
                cout << temp << "\n";
                myIter->getNext();
            }
        }
    }
    cout << "# of records in this tree: " << count << "\n";

}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
//        cout << "compare with int\n";
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}

void MyDB_BPlusTreeReaderWriter :: initialize() {
    forMe->setLastPage (0);
    //create root page (anonymous)
    cout << "create root page\n";
    root = make_shared <MyDB_PageReaderWriter> (*this, forMe->lastPage ());
    getTable()->setRootLocation(forMe->lastPage());
    root->clear ();
    cout << "type: " << root->getType() << " should be "<< DirectoryPage << "\n";
    root->setType(MyDB_PageType::DirectoryPage);
    cout << "type: " << root->getType() << " should be "<< DirectoryPage << "\n";
    
    
    //create leaf page (has directory)
    cout << "create leaf page\n";
    forMe->setLastPage(forMe->lastPage() + 1);
    MyDB_PageReaderWriterPtr leaf = make_shared <MyDB_PageReaderWriter> (*this, forMe->lastPage ());
    leaf->clear ();
    cout << "set leaf loc: " << forMe->lastPage() << "\n";
    
    //set ptr and default record key for root
    cout << "set ptr: " << forMe->lastPage() << "\n";
    MyDB_INRecordPtr record = getINRecord();
    record->setPtr(forMe->lastPage ());
    root->append(record);
    
    // and the root location
    rootLocation = getTable ()->getRootLocation ();
    cout << "--------------some tests below ----------------\n";
    cout << "type: " << root->getType() << " should be "<< DirectoryPage << "\n";
    cout << "root loc: " << getTable()->getRootLocation() << "\n";
    MyDB_INRecordPtr current = this->getINRecord();
    MyDB_RecordIteratorPtr myIter = root->getIterator(current);
    while (myIter->hasNext()) {
        myIter->getNext();
    }
    int index = current->getPtr();
    cout << "root rec1 points to: " << index << "\n";
    cout << "type: " << root->getType() << " should be "<< DirectoryPage << "\n";
}

MyDB_PageReaderWriterPtr MyDB_BPlusTreeReaderWriter :: getByID(int index) {
    //cout << "go to ptr: " << index << "\n";
    //see if we are going off of the end of the file... if so, then clear those pages
    while (index > forMe->lastPage ()) {
        forMe->setLastPage (forMe->lastPage () + 1);
        lastPage = make_shared <MyDB_PageReaderWriter> (*this, forMe->lastPage ());
        lastPage->clear ();
    }
    // now get the page
    MyDB_PageReaderWriterPtr child = make_shared <MyDB_PageReaderWriter> (*this, index);
    return child;
}

void MyDB_BPlusTreeReaderWriter :: sortPage(MyDB_PageReaderWriterPtr page) {
    MyDB_INRecordPtr lhs = this->getINRecord();
    MyDB_INRecordPtr rhs = this->getINRecord();
    page->sortInPlace(buildComparator(lhs, rhs), lhs, rhs);
}

bool MyDB_BPlusTreeReaderWriter :: compareTwoRecords(MyDB_AttValPtr rec1, MyDB_AttValPtr rec2) {
    MyDB_INRecordPtr lhs = getINRecord();
    MyDB_INRecordPtr rhs = getINRecord();
    function <bool()> comparator = buildComparator(lhs, rhs);
    lhs->setKey(rec1);
    rhs->setKey(rec2);
    return comparator();
}
#endif
