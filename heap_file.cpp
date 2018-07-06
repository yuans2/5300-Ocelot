#include "heap_file.h"

HeapFile::HeapFile(std::string name) : DbFile(name), last(0), closed(true), db(_DB_ENV, 0)
{
	this->dbfilename = this->name + ".db";
}

void HeapFile::create()
{
	db_open(DB_CREATE);
}

void HeapFile::open()
{
	if (this->closed)
	{
		db_open();
		
		DB_BTREE_STAT db_stat;
		this->db.stat(NULL, (void*)&db_stat, 0);
		
		this->last = db_stat.bt_ndata;
	}
}

void HeapFile::close()
{
	if (!this->closed)
	{
		this->db.close(0);
		this->closed = true;
	}
}

SlottedPage* HeapFile::get_new()
{
	if (this->closed)
	{
		open();
	}
	
	char block[DbBlock::BLOCK_SZ];
	this->last++;
	Dbt data(block, sizeof(block));
	Dbt key(&this->last, sizeof(this->last));
	this->db.put(NULL, &key, &data, 0);
	
	Dbt* rdata = new Dbt();
	this->db.get(NULL, &key, rdata, 0);
	
	return new SlottedPage(*rdata, this->last, true);
}

SlottedPage* HeapFile::get(BlockID block_id)
{
	if (this->closed)
	{
		open();
	}
	
	Dbt key(&block_id, sizeof(block_id));
	Dbt* rdata = new Dbt();
	this->db.get(NULL, &key, rdata, 0);
	
	return new SlottedPage(*rdata, block_id, false);
}

void HeapFile::put(DbBlock* block)
{
	if (this->closed)
	{
		open();
	}
	
	BlockID block_id = block->get_block_id();
	Dbt key(&block_id, sizeof(block_id));
	this->db.put(NULL, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids()
{
	if (this->closed)
	{
		open();
	}
	
	BlockIDs* blockIDs = new BlockIDs();
	
	for(BlockID i = 1; i <= this->last; i++)
	{
		blockIDs->push_back(i);
	}
	
	return blockIDs;
}

void HeapFile::db_open(uint flags)
{
	this->db.set_message_stream(_DB_ENV->get_message_stream());
	this->db.set_error_stream(_DB_ENV->get_error_stream());
	this->db.set_re_len(DbBlock::BLOCK_SZ);
	this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0);
	this->closed = false;
}