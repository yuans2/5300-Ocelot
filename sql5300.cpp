#include <algorithm>
#include "utility.h"
#include "db_cxx.h"

const int MAXPATHLENGTH = 1024;

int main(int argc, char *argv[])
{
	if (argc != 2)
	{
		std::cout << "Usage: sql5300 DbEnvPath" << std::endl;
		exit(-1);
	}

	char realPath[MAXPATHLENGTH];
	char* resolvedPath = realpath(argv[1], realPath);

	if (resolvedPath == NULL)
	{
		std::cout << "Invalid DbEnvPath" << std::endl;
		exit(-1);
	}

	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(realPath, DB_CREATE | DB_INIT_MPOOL, 0);

	std::cout << "(sql5300: running with database environment at " << realPath << ")" << std::endl;

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
