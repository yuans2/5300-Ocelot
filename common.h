/**
 * @file common.h - common aliases for types.
 *
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
 
#pragma once

#include <utility>
#include <vector>
#include <map>

/*
 * Convenient aliases for types
 */
typedef u_int16_t u16;
typedef u_int16_t RecordID;
typedef u_int32_t BlockID;
typedef std::vector<RecordID> RecordIDs;
typedef std::vector<BlockID> BlockIDs;  // FIXME: will need to turn this into an iterator at some point

/**
 * @class ColumnAttribute - holds dataype and other info for a column
 */
class ColumnAttribute {
public:
	enum DataType {
		INT,
		TEXT
	};
   ColumnAttribute() : data_type(INT) {}
	ColumnAttribute(DataType data_type) : data_type(data_type) {}
	virtual ~ColumnAttribute() {}

	virtual DataType get_data_type() { return data_type; }
	virtual void set_data_type(DataType data_type) {this->data_type = data_type;}

protected:
	DataType data_type;
};


/**
 * @class Value - holds value for a field
 */
class Value {
public:
	ColumnAttribute::DataType data_type;
	int32_t n;
	std::string s;

	Value() : n(0) {data_type = ColumnAttribute::INT;}
	Value(int32_t n) : n(n) {data_type = ColumnAttribute::INT;}
	Value(std::string s) : s(s) {data_type = ColumnAttribute::TEXT; }
  
   bool operator==(const Value &other) const;
   bool operator!=(const Value &other) const; 

};

// More type aliases
typedef std::string Identifier;
typedef std::vector<Identifier> ColumnNames;
typedef std::vector<ColumnAttribute> ColumnAttributes;
typedef std::pair<BlockID, RecordID> Handle;
typedef std::vector<Handle> Handles;  // FIXME: will need to turn this into an iterator at some point
typedef std::map<Identifier, Value> ValueDict;
typedef std::vector<ValueDict*> ValueDicts;
