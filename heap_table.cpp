/**
 * @file->>heap_table.cpp - function definitions for HeapTable.
 * HeapTable
 *
 *  
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

#include <memory>
#include <cstring>
#include "heap_table.h"
#include<iostream>



using namespace std;
// Constructor
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes )
	: DbRelation(table_name, column_names, column_attributes)
{
this->file = new HeapFile(table_name);
}

/**
 * Execute: CREATE TABLE <table_name> ( <columns> )
 */
void HeapTable::create()
{
	this->file->create();
}

/**
 * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
 */
void HeapTable::create_if_not_exists()
{
	try
	{
		this->file->create();
	}
	catch (DbException exception)
	{
		if (exception.get_errno() != EEXIST)
		{
			throw exception;
		}
	}
}

/**
 * Execute: DROP TABLE <table_name>
 */
void HeapTable::drop()
{
	this->file->drop();
}

/**
 * Open existing table to insert, update, select etc.
 */
void HeapTable::open()
{
	this->file->open();
}

/**
 * Closes an open table.
 */
void HeapTable::close()
{
	this->file->close();
}

/**
 * Execute: INSERT INTO <table_name> ( <row_keys> ) VALUES ( <row_values> )
 * @param row  a dictionary keyed by column names
 * @returns    a handle to the new row
 */
Handle HeapTable::insert(const ValueDict* row)
{
	validate(row);
	Dbt* data = marshal(row);
	
	u_int32_t last_block_id = this->file->get_last_block_id();
	
	SlottedPage* slotted_page = NULL;
	
	if (last_block_id == 0)
	{
		slotted_page = this->file->get_new();
	}
	else
	{
		slotted_page = this->file->get(last_block_id);
	}
	
	RecordID record_id;
	
	try
	{
		record_id = slotted_page->add(data);
	}
	catch(DbBlockNoRoomError db_block_no_room_error)
	{
		delete slotted_page;
		slotted_page = this->file->get_new();
		record_id = slotted_page->add(data);
	}
	
	this->file->put(slotted_page);
	
	Handle handle = std::make_pair(slotted_page->get_block_id(), record_id);
	
	delete slotted_page;
	delete[] (char*)data->get_data();
	delete data;
	
	return handle;
}

/**
 * Execute: UPDATE INTO <table_name> SET <new_valus> WHERE <handle>
 * where handle is sufficient to identify one specific record (e.g., returned
 * from an insert or select).
 * @param handle      the row to update
 * @param new_values  a dictionary keyd by column names for changing columns
 */
void HeapTable::update(const Handle handle, const ValueDict* new_values)
{
	validate(new_values);
	std::unique_ptr<SlottedPage> slotted_page(this->file->get(handle.first));
	std::unique_ptr<Dbt> data(marshal(new_values));
	
	try
	{
		RecordID record_id = handle.second;
		slotted_page->put(record_id, *data);
		this->file->put(slotted_page.get());
	}
	catch(DbBlockError exception)
	{
		delete[] (char*)data->get_data();
		
		throw exception;
	}
	
	delete[] (char*)data->get_data();
}
/**
 * Execute: DELETE FROM <table_name> WHERE <handle>
 * where handle is sufficient to identify one specific record (e.g, returned
 * from an insert or select).
 * @param handle   the row to delete
 */ 
void HeapTable::del(const Handle handle)
{
	std::unique_ptr<SlottedPage> slotted_page(this->file->get(handle.first));
	slotted_page->del(handle.second);
	this->file->put(slotted_page.get());
}


Handles* HeapTable::select() {
   return select(nullptr);
}

// Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
// // Returns a list of handles for qualifying rows.
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
            handles->push_back(handle);
      }
      delete record_ids;
      delete block;
    }
    delete block_ids;
   return handles;
}

// See if the row at the given handle satisfies the given where clause
bool HeapTable::selected(Handle handle, const ValueDict* where) {
   if (where == nullptr)
      return true;
   ValueDict* row = this->project(handle, where);
   return *row == *where;
}
/**
 * Execute: SELECT <handle> FROM <table_name> WHERE ...
 * @returns  a pointer to a list of handles for qualifying rows 
 */


// Just pulls out the column names from a ValueDict and passes that to the usual form of project().
ValueDict* HeapTable::project(Handle handle, const ValueDict* where) {
   ColumnNames t;
   for (auto const& column: *where)
      t.push_back(column.first);
   return this->project(handle, &t);
}



/*
 * Return a sequence of all values for handle (SELECT *).
 * @param handle  row to get values from
 * @returns       dictionary of values from row (keyed by all column names)
 */
ValueDict* HeapTable::project(Handle handle)
{
	std::unique_ptr<SlottedPage> slotted_page(this->file->get(handle.first));
	Dbt* record = slotted_page->get(handle.second);
	ValueDict* current_value = unmarshal(record);
	
	delete record;
	
	return current_value;
}

/**
 * Return a sequence of values for handle given by column_names 
 * (SELECT <column_names>).
 * @param handle        row to get values from
 * @param column_names  list of column names to project
 * @returns             dictionary of values from row (keyed by column_names)
 */
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names)
{
	ValueDict* all_values = project(handle);
	ValueDict* projected_values = new ValueDict();
	
	for(uint i = 0; i < column_names->size(); i++)
	{
		Identifier identifier = column_names->at(i);
		projected_values->insert(std::pair<Identifier, Value>(identifier, all_values->at(identifier)));
	}
	
	delete all_values;
	
	return projected_values;
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

// Assumes row is fully fleshed-out. Appends a record to the file->>
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
		} else if(ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            if(offset +1 > DbBlock::BLOCK_SZ - 1)
                throw DbRelationError("ROW TOO BIG TO MARSHAL");
            *(uint8_t*)(bytes+offset) = (uint8_t)value.n;
            offset += sizeof(uint8_t);

            //value.n = *(uint8_t*)(bytes + offset);
            //offset += sizeof(uint8_t);
        }else {
			throw DbRelationError("Only know how to marshal INT, TEXT, and BOOLEAN");
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
    	} else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN){
            value.n = *(uint8_t*)(bytes + offset);
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
    	}
		(*row)[column_name] = value;
    }
    return row;
}

