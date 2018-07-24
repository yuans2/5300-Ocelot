/**
 * @file db_file.h - Db file abstract class.
 * DbFile
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
 
#pragma once

#include "common.h"
#include "db_block.h"

/**
 * @class DbFileError - generic exception class for DbFile
 */
class DbFileError : public std::runtime_error {
public:
	explicit DbFileError(std::string s) : runtime_error(s) {}
};

/**
 * @class DbFile - abstract base class which represents a disk-based collection of DbBlocks
 * 	create()
 * 	drop()
 * 	open()
 * 	close()
 * 	get_new()
 *	get(block_id)
 *	put(block)
 *	block_ids()
 */
class DbFile {
public:
	// ctor/dtor -- subclasses should handle big-5
	DbFile(std::string name) : name(name) {}
	virtual ~DbFile() {}

	/**
	 * Create the file.
	 */	
	virtual void create() = 0;

	/**
	 * Remove the file.
	 */
	virtual void drop() = 0;

	/**
	 * Open the file.
	 */
	virtual void open() = 0;

	/**
	 * Close the file.
	 */
	virtual void close() = 0;

	/**
	 * Add a new block for this file.
	 * @returns  the newly appended block
	 */
	virtual DbBlock* get_new() = 0;

	/**
	 * Get a specific block in this file.
	 * @param block_id  which block to get
	 * @returns         pointer to the DbBlock (freed by caller)
	 */
	virtual DbBlock* get(BlockID block_id) = 0;

	/**
	 * Write a block to this file (the block knows its BlockID)
	 * @param block  block to write (overwrites existing block on disk)
	 */
	virtual void put(DbBlock* block) = 0;

	/**
	 * Get a list of all the valid BlockID's in the file
	 * FIXME - not a good long-term approach, but we'll do this until we put in iterators
	 * @returns  a pointer to vector of BlockIDs (freed by caller)
	 */ 
	virtual BlockIDs* block_ids() const = 0;

protected:
	std::string name;  // filename (or part of it)
};
