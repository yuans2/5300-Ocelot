/**
 * @file heap_file.cpp - function definitions for HeapFile.
 * HeapFile
 *
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

#include <cstring>
#include "heap_file.h"
#include<iostream>

using namespace std;
 /**
  * Constructor
  */

/*
 * *******************
 * HeapFile class
 * *******************
 */
 /**
  * Constructor
  */
HeapFile::HeapFile(std::string name) : DbFile(name), last(0), closed(true), db(_DB_ENV, 0)
{
	this->dbfilename = this->name + ".db";
}

/**
 * Create a file
 */
void HeapFile::create()
{
	db_open(DB_CREATE | DB_EXCL);
   SlottedPage *page = get_new();
   delete page;
}

/**
 * Remove the file
 */
void HeapFile::drop()
{
	if (!this->closed)
	{
		close();
	}
	
	_DB_ENV->dbremove(NULL, this->dbfilename.c_str(), NULL, 0);
}

/**
 * Open a file
 */
void HeapFile::open()
{
	if (this->closed)
		db_open();
}

/**
 * Close underlying file
 */
void HeapFile::close()
{
	if (!this->closed)
	{
		this->db.close(0);
		this->closed = true;
	}
}

/**
 * Add a new block to this file
 * @return newly added slotted page
 */
SlottedPage* HeapFile::get_new()
{	
	char block[DbBlock::BLOCK_SZ];
	std::memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));
	
	int block_id = ++this->last;
	Dbt key(&block_id, sizeof(block_id));

	this->db.put(NULL, &key, &data, 0);
	this->db.get(NULL, &key, &data, 0);
	
	return new SlottedPage(data, this->last, true);
}

/**
 * Get a specific block from this file
 * @param block_id the id of the block wanted
 * @return block asked
 */
SlottedPage* HeapFile::get(BlockID block_id)
{
	Dbt key(&block_id, sizeof(block_id));
	Dbt rdata;
	this->db.get(NULL, &key, &rdata, 0);
	
	return new SlottedPage(rdata, block_id, false);
}

/**
 * Write a block to this file
 * @param block to overwrite
 */
void HeapFile::put(DbBlock* block)
{	
	BlockID block_id = block->get_block_id();
	Dbt key(&block_id, sizeof(block_id));
	this->db.put(NULL, &key, block->get_block(), 0);
}

/**
 * Get the list of all the valid block ids in the file
 * @return a list of block ids 
 */
BlockIDs* HeapFile::block_ids() const
{

	BlockIDs* blockIDs = new BlockIDs();
	
	for(BlockID i = 1; i <= this->last; i++)
	{
		blockIDs->push_back(i);
	}
	
	return blockIDs;
}
uint32_t HeapFile::get_block_count() {
	DB_BTREE_STAT* stat;
	this->db.stat(nullptr, &stat, DB_FAST_STAT);
	return stat->bt_ndata;
}
/**
 * Opens the datebase represented by the file
 * @param flags flag parameter of the database
 */
void HeapFile::db_open(uint flags)
{
	if (this->closed)
	{
		this->db.set_re_len(DbBlock::BLOCK_SZ);
		this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0);
		this->closed = false;
      this->last = flags ? 0 : get_block_count();
	}
		else
		{
			throw DbFileError("The file was opened by this heap file object. Please use a new object to open again.");
		}
}
