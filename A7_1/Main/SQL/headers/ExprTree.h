
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include <string>
#include <vector>
#include <set>

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {

public:
	virtual string toString () = 0;
    virtual set<string> getTables () = 0;
    virtual vector<pair<string, string>> getAttsTables() = 0;
    virtual string getType () = 0;
    virtual ExprTreePtr getChild () = 0;
	virtual ~ExprTree () {}
};

class BoolLiteral : public ExprTree {

private:
	bool myVal;
public:
	
	BoolLiteral (bool fromMe) {
		myVal = fromMe;
	}

	string toString () {
		if (myVal) {
			return "bool[true]";
		} else {
			return "bool[false]";
		}
	}
    set<string> getTables () {
        set<string> ret;
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
    ~BoolLiteral () {}
};

class DoubleLiteral : public ExprTree {

private:
	double myVal;
public:

	DoubleLiteral (double fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "double[" + to_string (myVal) + "]";
	}	
    set<string> getTables () {
        set<string> ret;
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~DoubleLiteral () {}
};

// this implement class ExprTree
class IntLiteral : public ExprTree {

private:
	int myVal;
public:

	IntLiteral (int fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "int[" + to_string (myVal) + "]";
	}
    set<string> getTables () {
        set<string> ret;
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~IntLiteral () {}
};

class StringLiteral : public ExprTree {

private:
	string myVal;
public:

	StringLiteral (char *fromMe) {
		fromMe[strlen (fromMe) - 1] = 0;
		myVal = string (fromMe + 1);
	}

	string toString () {
		return "string[" + myVal + "]";
	}
    set<string> getTables () {
        set<string> ret;
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~StringLiteral () {}
};

class Identifier : public ExprTree {

private:
	string tableName;
	string attName;
public:

	Identifier (char *tableNameIn, char *attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
	}
    
	string toString () {
		return "[" + attName + "]";
	}
    
    set<string> getTables () {
        set<string> ret;
        ret.insert(tableName);
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        ret.push_back(make_pair(attName, tableName));
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~Identifier () {}
};

class MinusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	MinusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "- (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    set<string> getTables () {
        set<string> ret;
        set<string> lt = lhs->getTables();
        std::set<string>::iterator it;
        for (it=lt.begin(); it!=lt.end(); ++it)
            ret.insert(*it);
        
        set<string> rt = rhs->getTables();
        std::set<string>::iterator it2;
        for (it2=rt.begin(); it2!=rt.end(); ++it2)
            ret.insert(*it2);
        
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        for (pair<string, string> p: lhs->getAttsTables()) {
            ret.push_back(p);
        }
        for (pair<string, string> p: rhs->getAttsTables()) {
            ret.push_back(p);
        }
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~MinusOp () {}
};

class PlusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	PlusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "+ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    set<string> getTables () {
        set<string> ret;
        set<string> lt = lhs->getTables();
        std::set<string>::iterator it;
        for (it=lt.begin(); it!=lt.end(); ++it)
            ret.insert(*it);
        
        set<string> rt = rhs->getTables();
        std::set<string>::iterator it2;
        for (it2=rt.begin(); it2!=rt.end(); ++it2)
            ret.insert(*it2);
        
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        for (pair<string, string> p: lhs->getAttsTables()) {
            ret.push_back(p);
        }
        for (pair<string, string> p: rhs->getAttsTables()) {
            ret.push_back(p);
        }
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
    ~PlusOp () {}
};

class TimesOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	TimesOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "* (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    set<string> getTables () {
        set<string> ret;
        set<string> lt = lhs->getTables();
        std::set<string>::iterator it;
        for (it=lt.begin(); it!=lt.end(); ++it)
            ret.insert(*it);
        
        set<string> rt = rhs->getTables();
        std::set<string>::iterator it2;
        for (it2=rt.begin(); it2!=rt.end(); ++it2)
            ret.insert(*it2);
        
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        for (pair<string, string> p: lhs->getAttsTables()) {
            ret.push_back(p);
        }
        for (pair<string, string> p: rhs->getAttsTables()) {
            ret.push_back(p);
        }
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~TimesOp () {}
};

class DivideOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	DivideOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "/ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    set<string> getTables () {
        set<string> ret;
        set<string> lt = lhs->getTables();
        std::set<string>::iterator it;
        for (it=lt.begin(); it!=lt.end(); ++it)
            ret.insert(*it);
        
        set<string> rt = rhs->getTables();
        std::set<string>::iterator it2;
        for (it2=rt.begin(); it2!=rt.end(); ++it2)
            ret.insert(*it2);
        
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        for (pair<string, string> p: lhs->getAttsTables()) {
            ret.push_back(p);
        }
        for (pair<string, string> p: rhs->getAttsTables()) {
            ret.push_back(p);
        }
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~DivideOp () {}
};

class GtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	GtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "> (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    set<string> getTables () {
        set<string> ret;
        set<string> lt = lhs->getTables();
        std::set<string>::iterator it;
        for (it=lt.begin(); it!=lt.end(); ++it)
            ret.insert(*it);
        
        set<string> rt = rhs->getTables();
        std::set<string>::iterator it2;
        for (it2=rt.begin(); it2!=rt.end(); ++it2)
            ret.insert(*it2);
        
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        for (pair<string, string> p: lhs->getAttsTables()) {
            ret.push_back(p);
        }
        for (pair<string, string> p: rhs->getAttsTables()) {
            ret.push_back(p);
        }
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~GtOp () {}
};

class LtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	LtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "< (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    set<string> getTables () {
        set<string> ret;
        set<string> lt = lhs->getTables();
        std::set<string>::iterator it;
        for (it=lt.begin(); it!=lt.end(); ++it)
            ret.insert(*it);
        
        set<string> rt = rhs->getTables();
        std::set<string>::iterator it2;
        for (it2=rt.begin(); it2!=rt.end(); ++it2)
            ret.insert(*it2);
        
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        for (pair<string, string> p: lhs->getAttsTables()) {
            ret.push_back(p);
        }
        for (pair<string, string> p: rhs->getAttsTables()) {
            ret.push_back(p);
        }
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~LtOp () {}
};

class NeqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	NeqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}
    set<string> getTables () {
        set<string> ret;
        set<string> lt = lhs->getTables();
        std::set<string>::iterator it;
        for (it=lt.begin(); it!=lt.end(); ++it)
            ret.insert(*it);
        
        set<string> rt = rhs->getTables();
        std::set<string>::iterator it2;
        for (it2=rt.begin(); it2!=rt.end(); ++it2)
            ret.insert(*it2);
        
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        for (pair<string, string> p: lhs->getAttsTables()) {
            ret.push_back(p);
        }
        for (pair<string, string> p: rhs->getAttsTables()) {
            ret.push_back(p);
        }
        return ret;
    }
	string toString () {
		return "!= (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
    ~NeqOp () {}
};

class OrOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	OrOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "|| (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    set<string> getTables () {
        set<string> ret;
        set<string> lt = lhs->getTables();
        std::set<string>::iterator it;
        for (it=lt.begin(); it!=lt.end(); ++it)
            ret.insert(*it);
        
        set<string> rt = rhs->getTables();
        std::set<string>::iterator it2;
        for (it2=rt.begin(); it2!=rt.end(); ++it2)
            ret.insert(*it2);
        
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        for (pair<string, string> p: lhs->getAttsTables()) {
            ret.push_back(p);
        }
        for (pair<string, string> p: rhs->getAttsTables()) {
            ret.push_back(p);
        }
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~OrOp () {}
};

class EqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	EqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "== (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    set<string> getTables () {
        set<string> ret;
        set<string> lt = lhs->getTables();
        std::set<string>::iterator it;
        for (it=lt.begin(); it!=lt.end(); ++it)
            ret.insert(*it);
        
        set<string> rt = rhs->getTables();
        std::set<string>::iterator it2;
        for (it2=rt.begin(); it2!=rt.end(); ++it2)
            ret.insert(*it2);
        
        return ret;
    }
    vector<pair<string, string>> getAttsTables () {
        vector<pair<string, string>> ret;
        for (pair<string, string> p: lhs->getAttsTables()) {
            ret.push_back(p);
        }
        for (pair<string, string> p: rhs->getAttsTables()) {
            ret.push_back(p);
        }
        return ret;
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
    ~EqOp () {}
};

class NotOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	NotOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "!(" + child->toString () + ")";
	}	
    set<string> getTables () {
        
        return child->getTables();
    }
    vector<pair<string, string>> getAttsTables () {
        
        return child->getAttsTables();
    }
    string getType () {
        return "regular";
    }
    ExprTreePtr getChild () {
        return nullptr;
    }
	~NotOp () {}
};

class SumOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	SumOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "sum(" + child->toString () + ")";
	}	
    set<string> getTables () {
        
        return child->getTables();
    }
    vector<pair<string, string>> getAttsTables () {
        
        return child->getAttsTables();
    }
    string getType () {
        return "sum";
    }
    ExprTreePtr getChild () {
        return child;
    }
	~SumOp () {}
};

class AvgOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	AvgOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "avg(" + child->toString () + ")";
	}	
    set<string> getTables () {
        
        return child->getTables();
    }
    vector<pair<string, string>> getAttsTables () {
        
        return child->getAttsTables();
    }
    string getType () {
        return "avg";
    }
    ExprTreePtr getChild () {
        return child;
    }
	~AvgOp () {}
};

#endif
