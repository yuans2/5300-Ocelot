/**
 * @file sql5300.cpp - main entry for the relation manaager's SQL shell
 * @see "Seattle University, cpsc4300/5300, summer 2018"
 */

#include <algorithm>
#include "helper.h"
#include "db_cxx.h"

const int MAXPATHLENGTH = 1024;
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

	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(real_path, DB_CREATE | DB_INIT_MPOOL, 0);

	std::cout << "(sql5300: running with database environment at " << real_path << ")" << std::endl;

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
		else
		{
			std::cout << execute(query) << std::endl;
		}
	}
}
