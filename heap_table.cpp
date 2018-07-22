/**
 * @file heap_table.cpp - function definitions for HeapTable.
 * HeapTable
 *
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

#include <memory>
#include <cstring>
#include "heap_table.h"

// Constructor
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes )
	: DbRelation(table_name, column_names, column_attributes), file(table_name)
{
}

/**
 * Execute: CREATE TABLE <table_name> ( <columns> )
 */
void HeapTable::create()
{
	this->file.create();
}

/**
 * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
 */
void HeapTable::create_if_not_exists()
{
	try
	{
		this->file.create();
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
	this->file.drop();
}

/**
 * Open existing table to insert, update, select etc.
 */
void HeapTable::open()
{
	this->file.open();
}

/**
 * Closes an open table.
 */
void HeapTable::close()
{
	this->file.close();
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
	
	u_int32_t last_block_id = this->file.get_last_block_id();
	
	SlottedPage* slotted_page = NULL;
	
	if (last_block_id == 0)
	{
		slotted_page = this->file.get_new();
	}
	else
	{
		slotted_page = this->file.get(last_block_id);
	}
	
	RecordID record_id;
	
	try
	{
		record_id = slotted_page->add(data);
	}
	catch(DbBlockNoRoomError db_block_no_room_error)
	{
		delete slotted_page;
		slotted_page = this->file.get_new();
		record_id = slotted_page->add(data);
	}
	
	this->file.put(slotted_page);
	
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
	std::unique_ptr<SlottedPage> slotted_page(get_block(handle.first));
	std::unique_ptr<Dbt> data(marshal(new_values));
	
	try
	{
		RecordID record_id = handle.second;
		slotted_page->put(record_id, *data);
		this->file.put(slotted_page.get());
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
	std::unique_ptr<SlottedPage> slotted_page(get_block(handle.first));
	slotted_page->del(handle.second);
	this->file.put(slotted_page.get());
}


Handles* HeapTable::select() {
   return select(nullptr);
}

// Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
// // Returns a list of handles for qualifying rows.
Handles* HeapTable::select(const ValueDict* where) {
   open();
   Handles* handles = new Handles();
   BlockIDs* block_ids = file.block_ids();
   for (auto const& block_id: *block_ids) {
      SlottedPage* block = file.get(block_id);
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
/*
Handles* HeapTable::select()
{
	BlockIDs* block_ids = this->file.block_ids();
	Handles* handles = new Handles();
	
	for(uint i = 0; i < block_ids->size(); i++)
	{
		SlottedPage* slotted_page = get_block(block_ids->at(i));
		RecordIDs* record_ids = slotted_page->ids();
		
		for(uint j = 0; j < record_ids->size(); j++)
		{
			handles->push_back(std::make_pair(block_ids->at(i), record_ids->at(j)));
		}
		
		delete slotted_page;
		delete record_ids;
	}
	
	delete block_ids;
	
	return handles;
}
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
	std::unique_ptr<SlottedPage> slotted_page(get_block(handle.first));
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

/**
 * Get a specific block from table 
 * @param block_id the id of the block asked
 * @return block asked 
 */
SlottedPage* HeapTable::get_block(BlockID block_id)
{
	if (block_id == 0 || block_id > this->file.get_last_block_id())
	{
		throw DbRelationError("Unknown block id");
	}
	
	return this->file.get(block_id);
}

/**
 * Validate that the row is leagal for the table
 * @param row a dictionary keyed by column names
 */
void HeapTable::validate(const ValueDict* row)
{
	if (row == NULL || row->size() == 0)
	{
		throw DbRelationError("The row is empty");
	}
	
	if (row->size() != this->column_names.size())
	{
		throw DbRelationError("Number of rows doesn't match current relation");
	}
	
	for(uint i = 0; i < this->column_names.size(); i++)
	{
		Identifier column_name = this->column_names[i];
		ColumnAttribute column_attribute = this->column_attributes[i];
		
		if (row->find(column_name) == row->end())
		{
			throw DbRelationError("Missing column: " + column_name);
		}
		
		if (row->at(column_name).data_type != column_attribute.get_data_type())
		{
			throw DbRelationError("Column attribute type for " + column_name + "is not correct");
		}
	}
}


/**
 * Serialization
 * @param row  a dictionary keyed by column names
 * @returns  serialized data 
 */
Dbt* HeapTable::marshal(const ValueDict* row)
{
	char* bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
	uint offset = 0;
	uint col_num = 0;
	for (auto const& column_name: this->column_names)
	{
		ColumnAttribute ca = this->column_attributes[col_num++];
		ValueDict::const_iterator column = row->find(column_name);
		Value value = column->second;
		if (ca.get_data_type() == ColumnAttribute::DataType::INT)
		{
			*(int32_t*)(bytes + offset) = value.n;
			offset += sizeof(int32_t);
		}
		else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
		{
			uint size = value.s.length();
			*((u16*)(bytes + offset)) = size;
			offset += sizeof(u16);
			std::memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
			offset += size;
		}
		else
		{
			throw DbRelationError("Only know how to marshal INT and TEXT");
		}
	}
	char *right_size_bytes = new char[offset];
	std::memcpy(right_size_bytes, bytes, offset);
	delete[] bytes;
	Dbt *data = new Dbt(right_size_bytes, offset);
	return data;
}

/**
 * Unserialization
 * @param stored data to unserialize
 * @returns unserialized dictionary keyed by column names
 */
ValueDict* HeapTable::unmarshal(Dbt* data)
{
	char* bytes = (char*)data->get_data();
	ValueDict* value_dict = new ValueDict();
	uint offset = 0;
	for (uint i = 0; i < this->column_names.size(); i++)
	{
		Identifier identifier = this->column_names[i];
		ColumnAttribute column_attribute = this->column_attributes[i];
		if (column_attribute.get_data_type() == ColumnAttribute::DataType::INT)
		{
			int32_t value = *(int32_t*)(bytes + offset);
			offset += sizeof(int32_t);
			(*value_dict)[identifier] = Value(value);
		}
		else if (column_attribute.get_data_type() == ColumnAttribute::DataType::TEXT)
		{
			u16 str_length = *(u16*)(bytes + offset);
			offset += sizeof(u16);
			char* buffer = new char[str_length + 1];
			memcpy(buffer, bytes + offset, str_length);
			buffer[str_length] = '\0';
			offset += str_length;
			std::string str_value(buffer);
			(*value_dict)[identifier] = Value(str_value);
			delete[] buffer;
		}
		else
		{
			throw DbRelationError("Only know how to unmarshal INT and TEXT");
		}
	}
	
	return value_dict;
}
