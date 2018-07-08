/**
 * @file storage_engine.h - Storage engine abstract classes.
 * DbBlock
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#pragma once

#include <exception>
#include <map>
#include <utility>
#include <vector>
#include "db_cxx.h"

typedef u_int16_t u16;

/*
 * Convenient aliases for types
 */
typedef u_int16_t RecordID;
typedef u_int32_t BlockID;
typedef std::vector<RecordID> RecordIDs;
typedef std::length_error DbBlockNoRoomError;

/**
 * @class DbBlockError - generic exception class for DbBlock
 */
class DbBlockError : public std::runtime_error {
public:
	explicit DbBlockError(std::string s) : runtime_error(s) {}
};

/**
 * @class DbBlock - abstract base class for blocks in our database files 
 * (DbBlock's belong to DbFile's.)
 * 
 * Methods for putting/getting records in blocks:
 * 	initialize_new()
 * 	add(data)
 * 	get(record_id)
 * 	put(record_id, data)
 * 	del(record_id)
 * 	ids()
 * Accessors:
 * 	get_block()
 * 	get_data()
 * 	get_block_id()
 */
class DbBlock {
public:
	/**
	 * our blocks are 4kB
	 */ 
	static const uint BLOCK_SZ = 4096;

	/**
	 * ctor/dtor (subclasses should handle the big-5)
	 */ 
	DbBlock(Dbt &block, BlockID block_id, bool is_new=false) : block(block), block_id(block_id) {}
	virtual ~DbBlock() {}

	/**
	 * Reinitialize this block to an empty new block.
	 */
	virtual void initialize_new() {}

	/**
	 * Add a new record to this block.
	 * @param data  the data to store for the new record
	 * @returns     the new RecordID for the new record
	 * @throws      DbBlockNoRoomError if insufficient room in the block
	 */
	virtual RecordID add(const Dbt* data) throw(DbBlockNoRoomError) = 0;

	/**
	 * Get a record from this block.
	 * @param record_id  which record to fetch
	 * @returns          the data stored for the given record
	 */
	virtual Dbt* get(RecordID record_id) throw(DbBlockError) = 0;

	/**
	 * Change the data stored for a record in this block.
	 * @param record_id  which record to update
	 * @param data       the new data to store for the given record
	 * @throws           DbBlockNoRoomError if insufficient room in the block 
	 *                   (old record is retained)
	 */
	virtual void put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError, DbBlockError) = 0;

	/**
	 * Delete a record from this block.
	 * @param record_id  which record to delete
	 */
	virtual void del(RecordID record_id) = 0;

	/**
	 * Get all the record ids in this block (excluding deleted ones).
	 * @returns  pointer to list of record ids (freed by caller)
	 */ 
	virtual RecordIDs* ids() = 0;

	/**
	 * Access the whole block's memory as a BerkeleyDB Dbt pointer.
	 * @returns  Dbt used by this block
	 */ 
	virtual Dbt* get_block() {return &block;}

	/**
	 * Access the whole block's memory within the BerkeleyDb Dbt.
	 * @returns  Raw byte stream of this block
	 */
	virtual void* get_data() {return block.get_data();}

	/**
	 * Get this block's BlockID within its DbFile.
	 * @returns this block's id
	 */
	virtual BlockID get_block_id() {return block_id;}

protected:
	Dbt block;
	BlockID block_id;
};