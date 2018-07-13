/**
 * @file slotted_page.h - heap file implementation of DbBlock.
 * SlottedPage: DbBlock
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#pragma once

#include "db_cxx.h"
#include "db_block.h"

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 *      Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.

        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.
 *
 */
class SlottedPage : public DbBlock {
public:
	SlottedPage(Dbt &block, BlockID block_id, bool is_new=false);
	// Big 5 - we only need the destructor, copy-ctor, move-ctor, and op= are unnecessary
	// but we delete them explicitly just to make sure we don't use them accidentally
	virtual ~SlottedPage() {}
	SlottedPage(const SlottedPage& other) = delete;
	SlottedPage(SlottedPage&& temp) = delete;
	SlottedPage& operator=(const SlottedPage& other) = delete;
	SlottedPage& operator=(SlottedPage& temp) = delete;

	/**
	 * Add a new record to this block.
	 * @param data  the data to store for the new record
	 * @returns     the new RecordID for the new record
	 * @throws      DbBlockNoRoomError if insufficient room in the block
	 */
	virtual RecordID add(const Dbt* data) throw(DbBlockNoRoomError);
	
	/**
	 * Get a record from this block.
	 * @param record_id  which record to fetch
	 * @returns          the data stored for the given record
	 * @throwa			 DbBlockError if the asking record is not exist
	 */
	virtual Dbt* get(RecordID record_id) throw(DbBlockError);
	/**
	 * Change the data stored for a record in this block.
	 * @param record_id  which record to update
	 * @param data       the new data to store for the given record
	 * @throws           DbBlockNoRoomError if insufficient room in the block 
	 *                   (old record is retained)
	 */
	virtual void put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError, DbBlockError);
	/**
	 * Delete a record from this block.
	 * @param record_id  which record to delete
	 */
	virtual void del(RecordID record_id);
	/**
	 * Get all the record ids in this block (excluding deleted ones).
	 * @returns  pointer to list of record ids (freed by caller)
	 */ 
	virtual RecordIDs* ids(void);

protected:
	u_int16_t num_records;
	u_int16_t end_free;
	/*
	 * get the size and location of a record
	 * @param size  the size of the record
	 * @param loc   the location of the record 
	 * @param id    the id of the record  
	 */
	virtual void get_header(u_int16_t &size, u_int16_t &loc, RecordID id=0);
	/*
	 * update header 
	 * @param size  the size of the record
	 * @param loc   the location of the record 
	 * @param id    the id of the record  
	 */
	virtual void put_header(RecordID id=0, u_int16_t size=0, u_int16_t loc=0);
	/**
	 * check if the block has enough room to add new record or to update a record
	 * @returns true/false to indicate if the block has enough room 
	 */
	virtual bool has_room(u_int16_t size);
	//virtual void slide(u_int16_t start, u_int16_t end);
	
	/**
	 * get the calue stored in a specific location of the block
	 * @param location of the block 
	 */
	virtual u_int16_t get_n(u_int16_t offset);
	/**
	 * put a specific value in a specific position of the block
	 * @param offset position
	 * @param n      value
	 */
	virtual void put_n(u_int16_t offset, u_int16_t n);
	/**
	 * access to a specific position of the block
	 * @param offset  position
	 * @returns       a pointer pointing to the asking position of the block 
	 */
	virtual void* address(u_int16_t offset);
	/**
	 * Ensure the asking record exists 
	 * @param  record_id  which record to check
	 * @throws DbBlockError if the asking record is not exist
	 */
	virtual void ensure_record_exist(RecordID record_id) throw (DbBlockError);
	/**
	 * Check if the block has a specific record 
	 * @param record_id which record to check
	 */
	virtual bool have_record(RecordID record_id);
	/**
	 * Shift corresponding records in the block
	 * @param begin_record_id the first record on the left of the deleted or updated record
	 * @shift_offset          the size to shift 
	 * @left 				  the direction to shift 
	 */
	virtual void shift_records(RecordID begin_record_id, u_int16_t shift_offset, bool left = true);
};