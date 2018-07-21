/**
 * @file heap_file.h - heap file implementation of DbFile.
 * HeapFile : DbFile
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

#pragma once

#include "db_cxx.h"
#include "slotted_page.h"
#include "db_block.h"
#include "db_relation.h"
#include "db_file.h"

/**
 * Global variable to hold dbenv.
 */
extern DbEnv* _DB_ENV;

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */
class HeapFile : public DbFile {
public:
	HeapFile(std::string name);
	virtual ~HeapFile() {}
	HeapFile(const HeapFile& other) = delete;
	HeapFile(HeapFile&& temp) = delete;
	HeapFile& operator=(const HeapFile& other) = delete;
	HeapFile& operator=(HeapFile&& temp) = delete;

	virtual void create(void);
	virtual void drop(void);
	virtual void open(void);
	virtual void close(void);
	virtual SlottedPage* get_new(void);
	virtual SlottedPage* get(BlockID block_id);
	virtual void put(DbBlock* block);
	virtual BlockIDs* block_ids();

	virtual u_int32_t get_last_block_id() {return last;}

protected:
	std::string dbfilename;
	u_int32_t last;
	bool closed;
	bool opened_before;
	Db db;
	virtual void db_open(uint flags=0);
};