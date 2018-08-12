/**
 * @file sql5300.cpp - main entry for the relation manaager's SQL shell
 * @see "Seattle University, cpsc4300/5300, summer 2018"
 */

#include <algorithm>

#include "db_cxx.h"
#include "SQLExec.h"
#include "btree.h"
#include "ParseTreeToString.h"

using namespace std;

const int MAXPATHLENGTH = 1024;

DbEnv* _DB_ENV;

/**
 * Main entry point of the program
 * @args dbenvpath the path to BerkeleyDB environment  
 */
int main(int argc, char *argv[])
{
	if (argc != 2)
	{
		std::cout << "Usage: sql5300 DbEnvPath" << std::endl;
		exit(-1);
	}

	char real_path[MAXPATHLENGTH];
	char* resolved_path = realpath(argv[1], real_path);

	if (resolved_path == NULL)
	{
		std::cout << "Invalid DbEnvPath" << std::endl;
		exit(-1);
	}

	DbEnv* env = new DbEnv(0U);
	env->set_message_stream(&std::cout);
	env->set_error_stream(&std::cerr);
   try {
	   env->open(real_path, DB_CREATE | DB_INIT_MPOOL, 0);
   } catch (DbException &exe) {
      cerr << "(sql5300: " << exe.what() << ")" << endl;
      return 1;
   }
	std::cout << "(sql5300: running with database environment at " << real_path << ")" << std::endl;
	
	std::cout << "Usage:" << std::endl;
	std::cout << "	Type SQL to get translated SQL back;" << std::endl;
	//std::cout << "	Type test_slotted_page to run SlottedPage unit test;" << std::endl;
	//std::cout << "	Type test_heap_file to run HeapFile unit test;" << std::endl;
	//std::cout << "	Type test_heap_table to run HeapTable unit test;" << std::endl;
	
	_DB_ENV = env;

   initialize_schema_tables();

	//SQL shell loop
	while (true)
	{
		std::cout << "SQL> ";

		std::string query;
		std::getline(std::cin, query);

		if (query.length() == 0)
		{
			continue;
		}

		// Convert to lower case
		std::transform(query.begin(), query.end(), query.begin(), ::tolower);

		if (query == "quit")
		{
			return 0;
		}
//		else if (query == "test_slotted_page")
//		{
//			test_slotted_page();
//			std::cout << "test_slotted_page pass" << std::endl;
//		}
//		else if (query == "test_heap_file")
//		{
//			test_heap_file();
//			std::cout << "test_heap_file pass" << std::endl;
//		}
//		else if (query == "test_heap_table")
//		{
//			test_heap_table();
//			std::cout << "test_heap_table pass" << std::endl;
//		}
		if (query == "test") {
			cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
			cout << "test_btree: "<< (test_btree() ? "ok" : "failed") << endl;
			continue;
		}
		else
		{
			hsql::SQLParserResult* parseResult = hsql::SQLParser::parseSQLString(query);
	      std::string message;
	      if (parseResult->isValid())
	      {
            for (uint i = 0; i < parseResult->size(); i++) {
               try {
                  cout << ParseTreeToString::statement(parseResult->getStatement(i)) << endl;
                  QueryResult *result = SQLExec::execute(parseResult->getStatement(i));
                  cout << *result << endl;
                  delete result;
               } catch (SQLExecError& e) {
                  cout << "Error: " << e.what() << endl;
               }
	         }
	      } else
	      {
	      	cout << "Invalid SQL: " << query << endl;
	      }
      delete parseResult;
	   }
   }
   return 0;
}
