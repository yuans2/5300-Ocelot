/**
 * @file heap_table.h - heap file implementation of DbRelation.
 * HeapTable : DbRelation
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

#pragma once

#include "db_block.h"
#include "db_relation.h"
#include "db_file.h"
#include "heap_file.h"


/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */
class HeapTable : public DbRelation {
public:
	HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes );
	virtual ~HeapTable() {}
	HeapTable(const HeapTable& other) = delete;
	HeapTable(HeapTable&& temp) = delete;
	HeapTable& operator=(const HeapTable& other) = delete;
	HeapTable& operator=(HeapTable&& temp) = delete;
	
	/**
	 * Execute: CREATE TABLE <table_name> ( <columns> )
	 */
	virtual void create();
	/**
	 * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
	 */
	virtual void create_if_not_exists();
	/**
	 * Execute: DROP TABLE <table_name>
	 */
	virtual void drop();

	/**
	 * Open existing table to insert, update, select etc.
	 */
	virtual void open();
	/**
	 * Closes an open table.
	 */
	virtual void close();
	/**
	 * Execute: INSERT INTO <table_name> ( <row_keys> ) VALUES ( <row_values> )
	 * @param row  a dictionary keyed by column names
	 * @returns    a handle to the new row
	 */
	virtual Handle insert(const ValueDict* row);
	/**
	 * Execute: UPDATE INTO <table_name> SET <new_valus> WHERE <handle>
	 * where handle is sufficient to identify one specific record (e.g., returned
	 * from an insert or select).
	 * @param handle      the row to update
	 * @param new_values  a dictionary keyd by column names for changing columns
	 */
	virtual void update(const Handle handle, const ValueDict* new_values);
	/**
	 * Execute: DELETE FROM <table_name> WHERE <handle>
	 * where handle is sufficient to identify one specific record (e.g, returned
	 * from an insert or select).
	 * @param handle   the row to delete
	 */ 
	virtual void del(const Handle handle);
	/**
	 * Execute: SELECT <handle> FROM <table_name> WHERE ...
	 * @returns  a pointer to a list of handles for qualifying rows 
	 */
	virtual Handles* select();
	/**
	 * Return a sequence of all values for handle (SELECT *).
	 * @param handle  row to get values from
	 * @returns       dictionary of values from row (keyed by all column names)
	 */
	virtual ValueDict* project(Handle handle);
	/**
	 * Return a sequence of values for handle given by column_names 
	 * (SELECT <column_names>).
	 * @param handle        row to get values from
	 * @param column_names  list of column names to project
	 * @returns             dictionary of values from row (keyed by column_names)
	 */
	virtual ValueDict* project(Handle handle, const ColumnNames* column_names);

protected:
	HeapFile file;
	/**
	 * Validate that the row is leagal for the table
	 * @param row a dictionary keyed by column names
	 */
	virtual void validate(const ValueDict* row);
	/**
	 * Get a specific block from table 
	 * @param block_id the id of the block asked
	 * @return block asked 
	 */
	virtual SlottedPage* get_block(BlockID block_id);
	
	/**
	 * Serialization
	 * @param row  a dictionary keyed by column names
	 * @returns  serialized data 
	 */
	virtual Dbt* marshal(const ValueDict* row);
	
	/**
	 * Unserialization
	 * @param stored data to unserialize
	 * @returns unserialized dictionary keyed by column names
	 */
	virtual ValueDict* unmarshal(Dbt* data);
};