#include <cstring>
#include <memory>
#include "slotted_page.h"
#include "db_cxx.h"

class test_fail_error : public std::runtime_error {
public:
	explicit test_fail_error(std::string s) : runtime_error(s) {}
};

void test_slotted_page_when_empty()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	std::unique_ptr<RecordIDs> record_ids(slotted_page.ids());
	
	if (!record_ids->empty())
	{
		throw test_fail_error("Newly created block should not have any record");
	}
}

void test_slotted_page_add()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	Dbt record((char*) "a", 2);
	
	// Filing out the space
	for (int i = 0; i < 682; i++)
	{
		slotted_page.add(&record);
	}
	
	try
	{
		slotted_page.add(&record);
		throw test_fail_error("Should throw exception when the block is full");
	}
	catch (DbBlockNoRoomError db_block_error)
	{	
	}
}

void test_slotted_page_get()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	try
	{
		slotted_page.get(1);
		throw test_fail_error("slotted_page get() failed");
	}
	catch (DbBlockError db_block_error)
	{	
	}
	
	Dbt record((char*)"HelloWorld", 11);
	slotted_page.add(&record);
	
	std::unique_ptr<Dbt> dbt(slotted_page.get(1));
	std::string value((char *) dbt->get_data());
	
	if (value != "HelloWorld")
	{
		throw test_fail_error("slotted_page get() failed");
	}
}

void test_slotted_page_put()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	Dbt record((char*)"HelloWorld", 11);
	
	try
	{
		slotted_page.put(1, record);
		throw test_fail_error("slotted_page put() failed");
	}
	catch (DbBlockError db_block_error)
	{	
	}
	
	slotted_page.add(&record);
	
	Dbt updated_record((char*)"HelloSeattleU", 14);
	slotted_page.put(1, updated_record);
	
	std::unique_ptr<Dbt> dbt(slotted_page.get(1));
	std::string value((char *) dbt->get_data());
	
	if (value != "HelloSeattleU")
	{
		throw test_fail_error("slotted_page put() failed");
	}
}

void test_slotted_page_del()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	try
	{
		slotted_page.del(1);
		throw test_fail_error("slotted_page del() failed");
	}
	catch (DbBlockError db_block_error)
	{	
	}
	
	Dbt record((char*)"HelloWorld", 11);
	slotted_page.add(&record);
	slotted_page.del(1);
	
	std::unique_ptr<RecordIDs> record_ids(slotted_page.ids());
	
	if (!record_ids->empty())
	{
		throw test_fail_error("slotted_page del() failed");
	}
}

void test_slotted_page_get_block_id()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	if (slotted_page.get_block_id() != 1)
	{
		throw test_fail_error("Use block_id=1 to create block, returned block_id != 1");
	}
}

void test_slotted_page_get_data()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	if (slotted_page.get_data() != block_space.get())
	{
		throw test_fail_error("slotted_page get_data() failed");
	}
}

void test_slotted_page_get_block()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	if (slotted_page.get_block()->get_data() != block_space.get())
	{
		throw test_fail_error("slotted_page get_block() failed");
	}
}

void test_slotted_page_with_old_block()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	Dbt record((char*)"HelloWorld", 11);
	slotted_page.add(&record);
	
	SlottedPage slotted_page2(block, 2, false);
	std::unique_ptr<Dbt> dbt(slotted_page2.get(1));
	std::string value((char *) dbt->get_data());
	
	if (value != "HelloWorld")
	{
		throw test_fail_error("slotted_page get() failed");
	}
}

void test_slotted_page_ids()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	Dbt record((char*)"HelloWorld", 11);
	slotted_page.add(&record);
	slotted_page.add(&record);
	
	std::unique_ptr<RecordIDs> record_ids(slotted_page.ids());
	
	if (record_ids->at(0) != 1)
	{
		throw test_fail_error("slotted_page ids() failed");
	}
	
	if (record_ids->at(1) != 2)
	{
		throw test_fail_error("slotted_page ids() failed");
	}
}

void test_slotted_page_near_full()
{
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	BlockID block_id = 1;
	
	SlottedPage slotted_page(block, block_id, true);
	
	Dbt record_1((char*) "a", 2);
	
	// Filing out the space
	for (int i = 0; i < 682; i++)
	{
		slotted_page.add(&record_1);
	}
	
	slotted_page.del(1);
	slotted_page.del(2);
	slotted_page.del(3);
	
	Dbt record_2((char*) "b", 2);
	slotted_page.add(&record_2);
	
	try
	{
		slotted_page.add(&record_2);
		throw test_fail_error("Should throw exception when the block is full");
	}
	catch (DbBlockNoRoomError db_block_error)
	{	
	}
	
	slotted_page.del(4);
	Dbt record_3((char*)"cde", 4);
	slotted_page.put(5, record_3);
	
	std::unique_ptr<Dbt> record_5_ptr(slotted_page.get(5));
	std::string record_3_value((char*)record_5_ptr->get_data());
	if (record_3_value != "cde")
	{
		throw test_fail_error("Value got doesn't match");
	}
	
	Dbt record_4((char*)"cdef", 5);
	
	try
	{
		slotted_page.put(5, record_4);
		throw test_fail_error("Should throw exception when the block is full");
	}
	catch (DbBlockNoRoomError db_block_error)
	{	
	}
}

int main()
{
	test_slotted_page_when_empty();
	test_slotted_page_add();
	test_slotted_page_get();
	test_slotted_page_put();
	test_slotted_page_del();
	test_slotted_page_with_old_block();
	test_slotted_page_ids();
	test_slotted_page_get_block();
	test_slotted_page_get_data();
	test_slotted_page_near_full();

	return 0;
}
