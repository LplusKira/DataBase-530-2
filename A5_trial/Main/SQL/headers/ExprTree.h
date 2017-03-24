
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include "MyDB_Catalog.h"
#include "MyDB_Schema.h"
#include "MyDB_Table.h"
#include <string>
#include <vector>

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {

public:
	virtual string toString () = 0;
	virtual ~ExprTree () {}
    virtual string type() = 0;
    virtual string check(MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) = 0;
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
    string type () {
        return "BoolLiteral";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        return "BoolLiteral";
    }
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
    string type () {
        return "DoubleLiteral";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        return "DoubleLiteral";
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
    string type () {
        return "IntLiteral";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        return "IntLiteral";
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
    string type () {
        return "StringLiteral";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        return "StringLiteral";
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
		return "[" + tableName + "@" + attName + "]";
	}
    
    string getTableName() {
        return tableName;
    }
    
    string getAttName() {
        return attName;
    }
    string type () {
        return "Identifier";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        cout << "check this att\n";
        string ret;
        string tableName_full;
        for (auto a: tablesToProcess) {
            if (a.second == tableName)
                tableName_full = a.first;
        }
        
        MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema> ();
        mySchema->fromCatalog (tableName_full, catalog);
        vector <pair <string, MyDB_AttTypePtr>> atts = mySchema->getAtts();
        bool attisthere = false;
        for (pair <string, MyDB_AttTypePtr> att: atts) {
            if (att.first == attName) {
                attisthere = true;
                cout << att.first << ", " << att.second->toString() << ".\n";
                if (att.second->toString() == "bool") {
                    ret = "BoolLiteral";
                } else if (att.second->toString() == "double") {
                    ret = "DoubleLiteral";
                } else if (att.second->toString() == "int"){
                    ret = "IntLiteral";
                } else {
                    ret = "StringLiteral";
                }
                break;
            }
        }
        if (!attisthere) {
            cout << "\n\nError: There is no att named ("<< attName << ") in table [" << tableName_full <<"].\n\n";
        }
        return ret;
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
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        string lht = lhs->check(catalog, tablesToProcess);
        string rht = rhs->check(catalog, tablesToProcess);
        if (rht == "StringLiteral" || lht == "StringLiteral" || rht == "ERROR" || lht == "ERROR") {
            cout << "ERROR: string text in MinusOp.\n";
            return "ERROR";
        }
        return "DoubleLiteral";
    }
	string toString () {
		return "- (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    string type () {
        return "MinusOp";
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
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        string lht = lhs->check(catalog, tablesToProcess);
        string rht = rhs->check(catalog, tablesToProcess);
        if (rht == "ERROR" || lht == "ERROR") {
            cout << "ERROR: either lht or rht is error in this plus statement.\n";
            return "ERROR";
        } else if (rht == "StringLiteral" || lht == "StringLiteral") {
            return "StringLiteral";
        } else if (rht == "IntLiteral" && lht == "IntLiteral") {
            return "IntLiteral";
        } else {
            return "DoubleLiteral";
        }
    }
	string toString () {
		return "+ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    string type () {
        return "PlusOp";
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
    string type () {
        return "TimesOp";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        string lht = lhs->check(catalog, tablesToProcess);
        string rht = rhs->check(catalog, tablesToProcess);
        if (rht == "ERROR" || lht == "ERROR") {
            cout << "ERROR: either lht or rht is error in this TimesOp.\n";
            return "ERROR";
        } else if (rht == "StringLiteral" || lht == "StringLiteral") {
            cout << "ERROR: string text in TimesOp.\n";
            return "ERROR";
        } else if (rht == "IntLiteral" && lht == "IntLiteral") {
            return "IntLiteral";
        } else {
            return "DoubleLiteral";
        }
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
    string type () {
        return "DivideOp";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        string lht = lhs->check(catalog, tablesToProcess);
        string rht = rhs->check(catalog, tablesToProcess);
        if (rht == "ERROR" || lht == "ERROR") {
            cout << "ERROR: either lht or rht is error in this DivideOp.\n";
            return "ERROR";
        } else if (rht == "StringLiteral" || lht == "StringLiteral") {
            cout << "ERROR: string text in DivideOp.\n";
            return "ERROR";
        } else {
            return "DoubleLiteral";
        }
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
    string type () {
        return "GtOp";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        string lht = lhs->check(catalog, tablesToProcess);
        string rht = rhs->check(catalog, tablesToProcess);
        if (rht == "ERROR" || lht == "ERROR") {
            cout << "ERROR: either lht or rht is error in this GtOp.\n";
            return "ERROR";
        } else if (rht == "StringLiteral" && lht == "StringLiteral") {
            return "BoolLiteral";
        } else if ((rht == "StringLiteral" && lht != "StringLiteral")
                   || (lht == "StringLiteral" && rht!= "StringLiteral")) {
            cout << "ERROR: string compared with double/int.\n";
            return "ERROR";
        } else {
            return "BoolLiteral";
        }
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
    string type () {
        return "LtOp";
    }
    string check (MyDB_CatalogPtr catalog,  vector <pair <string, string>> tablesToProcess) {
        string lht = lhs->check(catalog, tablesToProcess);
        string rht = rhs->check(catalog, tablesToProcess);
        if (rht == "ERROR" || lht == "ERROR") {
            cout << "ERROR: either lht or rht is error in this LtOp.\n";
            return "ERROR";
        } else if (rht == "StringLiteral" && lht == "StringLiteral") {
            return "BoolLiteral";
        } else if ((rht == "StringLiteral" && lht != "StringLiteral")
                   || (lht == "StringLiteral" && rht!= "StringLiteral")) {
            cout << "ERROR: string compared with double/int.\n";
            return "ERROR";
        } else {
            return "BoolLiteral";
        }
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

	string toString () {
		return "!= (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	
    string type () {
        return "NeqOp";
    }
    string check (MyDB_CatalogPtr catalog,  vector <pair <string, string>> tablesToProcess) {
        string lht = lhs->check(catalog, tablesToProcess);
        string rht = rhs->check(catalog, tablesToProcess);
        if (rht == "ERROR" || lht == "ERROR") {
            cout << "ERROR: either lht or rht is error in this NeqOp.\n";
            return "ERROR";
        } else if (rht == "StringLiteral" && lht == "StringLiteral") {
            return "BoolLiteral";
        } else if ((rht == "StringLiteral" && lht != "StringLiteral")
                   || (lht == "StringLiteral" && rht!= "StringLiteral")) {
            cout << "ERROR: string compared with double/int.\n";
            return "ERROR";
        } else {
            return "BoolLiteral";
        }
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
    string type () {
        return "OrOp";
    }
    string check (MyDB_CatalogPtr catalog,  vector <pair <string, string>> tablesToProcess) {
        string lht = lhs->check(catalog, tablesToProcess);
        string rht = rhs->check(catalog, tablesToProcess);
        if (rht == "BoolLiteral" && lht == "BoolLiteral") {
            return "BoolLiteral";
        } else {
            return "ERROR";
        }
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
    string type () {
        return "EqOp";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        string lht = lhs->check(catalog, tablesToProcess);
        string rht = rhs->check(catalog, tablesToProcess);
        if (rht == "ERROR" || lht == "ERROR") {
            cout << "ERROR: either lht or rht is error in this EqOp.\n";
            return "ERROR";
        } else if (rht == "StringLiteral" && lht == "StringLiteral") {
            return "BoolLiteral";
        } else if ((rht == "StringLiteral" && lht != "StringLiteral")
                   || (lht == "StringLiteral" && rht != "StringLiteral")){
            return "ERROR";
        } else {
            return "BoolLiteral";
        }
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
    string type () {
        return "NotOp";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        string ct = child->check(catalog, tablesToProcess);
        if (ct == "ERROR") {
            return "ERROR";
        } else {
            return "BoolLiteral";
        }
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
    string type () {
        return "SumOp";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        string ct = child->check(catalog, tablesToProcess);
        if (ct == "StringLiteral") {
            return "ERROR";
        }
        return ct;
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
    string type () {
        return "AvgOp";
    }
    string check (MyDB_CatalogPtr catalog, vector <pair <string, string>> tablesToProcess) {
        string ct = child->check(catalog, tablesToProcess);
        if (ct == "StringLiteral") {
            return "ERROR";
        }
        return ct;
    }
	~AvgOp () {}
};

#endif
