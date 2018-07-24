/**
 * @file db_relation.h - Db relation abstract class.
 * DbRelation
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

#pragma once

#include "common.h"
#include "db_file.h"


/**
 * @class DbRelationError - generic exception class for DbRelation
 */
class DbRelationError : public std::runtime_error {
public:
	explicit DbRelationError(std::string s) : runtime_error(s) {}
};

/**
 * @class DbRelation - top-level object handling a physical database relation
 * 
 * Methods:
 * 	create()
 * 	create_if_not_exists()
 * 	drop()
 * 	
 * 	open()
 * 	close()
 * 	
 *	insert(row)
 *	update(handle, new_values)
 *	del(handle)
 *	select()
 *	select(where)
 *	project(handle)
 *	project(handle, column_names)
 */
class DbRelation {
public:
	// ctor/dtor
	DbRelation(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes ) :
		table_name(table_name), column_names(column_names), column_attributes(column_attributes) {}
	virtual ~DbRelation() {}

	/**
	 * Execute: CREATE TABLE <table_name> ( <columns> )
	 * Assumes the metadata and validation are already done.
	 */
	virtual void create() = 0;

	/**
	 * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
	 * Assumes the metadata and validate are already done.
	 */
	virtual void create_if_not_exists() = 0;

	/**
	 * Execute: DROP TABLE <table_name>
	 */
	virtual void drop() = 0;

	/**
	 * Open existing table.
	 * Enables: insert, update, del, select, project.
	 */
	virtual void open() = 0;

	/**
	 * Closes an open table.
	 * Disables: insert, update, del, select, project.
	 */
	virtual void close() = 0;

	/**
	 * Execute: INSERT INTO <table_name> ( <row_keys> ) VALUES ( <row_values> )
	 * @param row  a dictionary keyed by column names
	 * @returns    a handle to the new row
	 */
	virtual Handle insert(const ValueDict* row) = 0;

	/**
	 * Conceptually, execute: UPDATE INTO <table_name> SET <new_valus> WHERE <handle>
	 * where handle is sufficient to identify one specific record (e.g., returned
	 * from an insert or select).
	 * @param handle      the row to update
	 * @param new_values  a dictionary keyd by column names for changing columns
	 */
	virtual void update(const Handle handle, const ValueDict* new_values) = 0;

	/**
	 * Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
	 * where handle is sufficient to identify one specific record (e.g, returned
	 * from an insert or select).
	 * @param handle   the row to delete
	 */ 
	virtual void del(const Handle handle) = 0;

	/**
	 * Conceptually, execute: SELECT <handle> FROM <table_name> WHERE 1
	 * @returns  a pointer to a list of handles for qualifying rows (caller frees)
	 */
	virtual Handles* select() = 0;

	/**
	 * Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
	 * @param where  where-clause predicates
	 * @returns      a pointer to a list of handles for qualifying rows (freed by caller)
	 */
	//virtual Handles* select(const ValueDict* where) = 0;
	virtual Handles* select(const ValueDict* where) = 0;



	/**
	 * Return a sequence of all values for handle (SELECT *).
	 * @param handle  row to get values from
	 * @returns       dictionary of values from row (keyed by all column names)
	 */
	virtual ValueDict* project(Handle handle) = 0;

	/**
	 * Return a sequence of values for handle given by column_names 
	 * (SELECT <column_names>).
	 * @param handle        row to get values from
	 * @param column_names  list of column names to project
	 * @returns             dictionary of values from row (keyed by column_names)
	 */
	virtual ValueDict* project(Handle handle, const ColumnNames* column_names) = 0;


	/**
	 * Return a sequence of values for handle given by column_names (from dictionary) 
	 * (SELECT <column_names>).
	 * @param handle        row to get values from
	 * @param column_names  list of column names to project (taken from keys of dict)
	 * @returns             dictionary of values from row (keyed by column_names)
	 */
	virtual ValueDict* project(Handle handle, const ValueDict* column_names);

	/**
	 * Accessor for column_names.
	 * @returns column_names   list of column names for this relation, in order
	 */
	virtual const ColumnNames& get_column_names() const { 
		return column_names; 
	}
	
protected:
	Identifier table_name;
	ColumnNames column_names;
	ColumnAttributes column_attributes;
};