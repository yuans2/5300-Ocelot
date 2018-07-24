/**
 * @file->heap_table.cpp - function definitions for HeapTable.
 * HeapTable
 *
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

#include <memory>
#include <cstring>
#include "heap_table.h"
#include<iostream>

using namespace std;
// Constructor
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes ) :
		DbRelation(table_name, column_names, column_attributes) {
   this->file = new HeapFile(table_name);
}

// Execute: CREATE TABLE <table_name> ( <columns> )
// Is not responsible for metadata storage or validation.
void HeapTable::create() {
	file->create();
}

// Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
// Is not responsible for metadata storage or validation.
void HeapTable::create_if_not_exists() {
	try {
		open();
	} catch (DbException& e) {
		create();
	}
}

// Execute: DROP TABLE <table_name>
void HeapTable::drop() {
	file->drop();
}

// Open existing table. Enables: insert, update, delete, select, project
void HeapTable::open() {
	file->open();
}

// Closes the table. Disables: insert, update, delete, select, project
void HeapTable::close() {
	file->close();
}

// Expect row to be a dictionary with column name keys.
// Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
// Return the handle of the inserted row.
Handle HeapTable::insert(const ValueDict* row) {
    open();
    ValueDict* full_row = validate(row);
    Handle handle = append(full_row);
    delete full_row;
    return handle;
}

// Expect new_values to be a dictionary with column name keys.
// Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <handle>
// where handle is sufficient to identify one specific record (e.g., returned from an insert
// or select).
void HeapTable::update(const Handle handle, const ValueDict* new_values) {
	throw DbRelationError("Not implemented");
}

// Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
// where handle is sufficient to identify one specific record (e.g., returned from an insert
// or select).
void HeapTable::del(const Handle handle) {
	open();
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	SlottedPage* block = this->file->get(block_id);
	block->del(record_id);
	this->file->put(block);
	delete block;
}

// Conceptually, execute: SELECT <handle> FROM <table_name> WHERE 1
// Returns a list of handles for qualifying rows.
Handles* HeapTable::select() {
	return select(nullptr);
}

// Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
// Returns a list of handles for qualifying rows.
Handles* HeapTable::select(const ValueDict* where) {
	open();
	Handles* handles = new Handles();
	BlockIDs* block_ids = file->block_ids();
    for (auto const& block_id: *block_ids) {
    	SlottedPage* block = file->get(block_id);
    	RecordIDs* record_ids = block->ids();
    	for (auto const& record_id: *record_ids) {
			Handle handle(block_id, record_id);
			if (selected(handle, where))
    			handles->push_back(Handle(block_id, record_id));
		}
    	delete record_ids;
    	delete block;
    }
    delete block_ids;
	return handles;
}

// Return a sequence of all values for handle.
ValueDict* HeapTable::project(Handle handle) {
	return project(handle, &this->column_names);
}

ValueDict* HeapTable::project(Handle handle, const ValueDict* where) {
   ColumnNames t;
   for (auto const& column: *where)
      t.push_back(column.first);
   return this->project(handle, &t);
}
// Return a sequence of values for handle given by column_names.
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
    SlottedPage* block = file->get(block_id);
    Dbt* data = block->get(record_id);
    ValueDict* row = unmarshal(data);
    delete data;
    delete block;
    if (column_names->empty())
    	return row;
    ValueDict* result = new ValueDict();
    for (auto const& column_name: *column_names) {
		if (row->find(column_name) == row->end())
			throw DbRelationError("table does not have column named '" + column_name + "'");
    	(*result)[column_name] = (*row)[column_name];
	}
	delete row;
    return result;
}

// Check if the given row is acceptable to insert. Raise ValueError if not.
// Otherwise return the full row dictionary.
ValueDict* HeapTable::validate(const ValueDict* row) const {
    ValueDict* full_row = new ValueDict();
    for (auto const& column_name: this->column_names) {
    	Value value;
    	ValueDict::const_iterator column = row->find(column_name);
    	if (column == row->end())
    		throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
    	else
    		value = column->second;
    	(*full_row)[column_name] = value;
    }
    return full_row;
}

// Assumes row is fully fleshed-out. Appends a record to the file->
Handle HeapTable::append(const ValueDict* row) {
    Dbt* data = marshal(row);
    SlottedPage* block = this->file->get(this->file->get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbBlockNoRoomError& e) {
    	// need a new block
    	block = this->file->get_new();
    	record_id = block->add(data);
    }
    this->file->put(block);
	delete block;
    delete[] (char*)data->get_data();
    delete data;
    return Handle(this->file->get_last_block_id(), record_id);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) const {
	char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
    	ColumnAttribute ca = this->column_attributes[col_num++];
    	ValueDict::const_iterator column = row->find(column_name);
		Value value = column->second;

		if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
			if (offset + 4 > DbBlock::BLOCK_SZ - 4)
				throw DbRelationError("row too big to marshal");
			*(int32_t*) (bytes + offset) = value.n;
			offset += sizeof(int32_t);
		} else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
			u_long size = value.s.length();
			if (size > UINT16_MAX)
				throw DbRelationError("text field too long to marshal");
			if (offset + 2 + size > DbBlock::BLOCK_SZ)
				throw DbRelationError("row too big to marshal");
			*(u16*) (bytes + offset) = size;
			offset += sizeof(u16);
			memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
			offset += size;
		} else {
			throw DbRelationError("Only know how to marshal INT and TEXT");
		}
	}
	char *right_size_bytes = new char[offset];
	memcpy(right_size_bytes, bytes, offset);
	delete[] bytes;
	Dbt *data = new Dbt(right_size_bytes, offset);
	return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data) const {
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char*)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
    	ColumnAttribute ca = this->column_attributes[col_num++];
		value.data_type = ca.get_data_type();
    	if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
    		value.n = *(int32_t*)(bytes + offset);
    		offset += sizeof(int32_t);
    	} else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
    		u16 size = *(u16*)(bytes + offset);
    		offset += sizeof(u16);
    		char buffer[DbBlock::BLOCK_SZ];
    		memcpy(buffer, bytes+offset, size);
    		buffer[size] = '\0';
    		value.s = string(buffer);  // assume ascii for now
            offset += size;
    	} else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
    	}
		(*row)[column_name] = value;
    }
    return row;
}

// See if the row at the given handle satisfies the given where clause
bool HeapTable::selected(Handle handle, const ValueDict* where) {
	if (where == nullptr)
		return true;
	ValueDict* row = this->project(handle, where);
	return *row == *where;
}


