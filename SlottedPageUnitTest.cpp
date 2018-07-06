#include <cstring>
#include "SlottedPage.h"
#include "db_cxx.h"

int main()
{
	char blockSpace[DbBlock::BLOCK_SZ];
	Dbt block(blockSpace, sizeof(blockSpace));
	
	SlottedPage slottedPage(block, 1, true);
	
	char* message = (char*)"HelloWorld";
	
	Dbt record(message, strlen(message));
	
	RecordID id = slottedPage.add(&record);
	
	Dbt* recordRead = slottedPage.get(id);
	std::cout << (char*) recordRead->get_data() << std::endl;
	
	return 0;
}