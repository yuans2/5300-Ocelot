#include <cstring>
#include "slotted_page.h"

typedef u_int16_t u16;

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
	if (is_new)
	{
		this->num_records = 0;
		this->end_free = DbBlock::BLOCK_SZ - 1;
		put_header();
	}
	else
	{
		get_header(this->num_records, this->end_free);
	}
}

RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError)
{
	if (!has_room(data->get_size()))
	{
		throw DbBlockNoRoomError("not enough room for new record");
	}

	u16 id = ++this->num_records;
	u16 size = (u16) data->get_size();
	this->end_free -= size;
	u16 loc = this->end_free + 1;

	put_header();
	put_header(id, size, loc);

	memcpy(this->address(loc), data->get_data(), size);

	return id;
}

Dbt* SlottedPage::get(RecordID record_id)
{
	if (!have_record(record_id))
	{
		return NULL;
	}

	u16 size, offset;
	get_header(size, offset, record_id);

	if (offset == 0)
	{
		return NULL;
	}

	// TODO decide whether use the memory of its block or create new one
	return new Dbt(this->address(offset), size);
}

bool SlottedPage::have_record(RecordID record_id)
{
	if (record_id == 0 || record_id > this->num_records)
	{
		return false;
	}

	u16 size, offset;
	get_header(size, offset, record_id);

	if (offset == 0)
	{
		return false;
	}

	return true;
}

void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError)
{
	if (!have_record(record_id))
	{
		return;
	}

	u16 old_size, old_offset;
	get_header(old_size, old_offset, record_id);
	u16 new_size = (u16) data.get_size();
	
	if (new_size> old_size && !has_room(new_size-old_size-4))
	{
		throw DbBlockNoRoomError("not enough room for updating record");
	}

	if (new_size > old_size)
	{
		u16 shift_offset = new_size - old_size;
		u16 new_offset = old_offset - shift_offset;

		shift_records(record_id + 1, shift_offset);
		memcpy(this->address(new_offset), data.get_data(), new_size);
		put_header(record_id, new_size, new_offset);	

		if (record_id == this->num_records)
		{
			this->end_free -= shift_offset;
		}
	}
	else
	{
		u16 shift_offset = old_size - new_size;
		u16 new_offset = old_offset + shift_offset;

		shift_records(record_id + 1, shift_offset, false);
		memcpy(this->address(new_offset), data.get_data(), new_size);
		put_header(record_id, new_size, new_offset);	

		if (record_id == this->num_records)
		{
			this->end_free += shift_offset;
		}
	}

	put_header();
}

void SlottedPage::del(RecordID record_id)
{
	if (!have_record(record_id))
	{
		return;
	}

	u16 size, offset;
	get_header(size, offset, record_id);

	shift_records(record_id + 1, size, false);
	put_header(record_id, 0U, 0U);

	if (record_id == this->num_records)
	{
		this->end_free += size;
	}

	put_header();
}

void SlottedPage::shift_records(RecordID begin_record_id, u_int16_t shift_offset, bool left)
{
	while(begin_record_id <= this->num_records && !have_record(begin_record_id))
	{
		begin_record_id++;
	}

	if (begin_record_id > this->num_records)
	{
		return;
	}

	u16 begin_offset, begin_size;
	get_header(begin_size, begin_offset, begin_record_id);

	u16 shift_block_size = begin_offset + begin_size - 1 - this->end_free;

	char temperary[shift_block_size];
	memcpy(temperary, this->address(this->end_free + 1), shift_block_size);
	
	if (left)
	{
		memcpy(this->address(this->end_free + 1 - shift_offset), temperary, shift_block_size);
	}
	else
	{
		memcpy(this->address(this->end_free + 1 + shift_offset), temperary, shift_block_size);
	}

	for(RecordID i = begin_record_id; i <= this->num_records; i++)
	{
		if (have_record(i))
		{
			u16 temp_offset, temp_size;
			get_header(temp_size, temp_offset, i);

			if (left)
			{
				put_header(i, temp_size, temp_offset-shift_offset);
			}
			else
			{
				put_header(i, temp_size, temp_offset+shift_offset);
			}
		}
	}

	this->end_free = left ? this->end_free - shift_offset : this->end_free + shift_offset;

	put_header();
}

RecordIDs* SlottedPage::ids(void)
{
	RecordIDs* record_ids = new RecordIDs();

	for (RecordID i = 1; i <= this->num_records; i++)
	{
		if (have_record(i))
		{
			record_ids->push_back(i);
		}
	}

	return record_ids;
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id)
{
	size = get_n(4*id);
	loc = get_n(4*id + 2);
}

void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc)
{
	if (id == 0)
	{
		// called the put_header() version and using the default params
		size = this->num_records;
		loc = this->end_free;
	}

	put_n(4*id, size);
	put_n(4*id + 2, loc);
}

bool SlottedPage::has_room(u_int16_t size)
{
	return 4 * (this->num_records + 1) + 3 < this->end_free - size + 1;
}

u16 SlottedPage::get_n(u16 offset)
{
	return *(u16*)this->address(offset);
}

void SlottedPage::put_n(u16 offset, u16 n)
{
	*(u16*)this->address(offset) = n;
}

void* SlottedPage::address(u16 offset)
{
	return (void*)((char*)this->block.get_data() + offset);
}