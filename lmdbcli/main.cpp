#include <iostream>

#include <boost/filesystem.hpp>

#include "LmdbClient.hpp"

using namespace std;

void help()
{
	cout << "[get \"key\" | set \"key\" \"value\" | del \"key\" | list] (path)" << endl;
}

void get(string path, string key)
{
	try
	{
		string value = LmdbClient::Get(path, key);
		cout << "\"" << key << "\"\t\"" << value << "\"" << endl;
	}
	catch (...)
	{
		cerr << "get failed" << endl;
	}
}

void set(string path, string key, string value)
{
	try
	{
		LmdbClient::Set(path, key, value);
		cout << "\"" << key << "\"\t\"" << value << "\"" << endl;
	}
	catch (...)
	{
		cerr << "set failed" << endl;
	}
}

void del(string path, string key)
{
	try
	{
		LmdbClient::Remove(path, key);
		cout << "\"" << key << "\" is deleted" << endl;
	}
	catch (...)
	{
		cerr << "del failed" << endl;
	}
}

void listc(string path)
{
	try
	{
		LmdbClient::Query(path, [&](const string &key, const string &value)
		{
			cout << "\"" << key << "\"\t\"" << value << "\"" << endl;
		});
	}
	catch (...)
	{
		cerr << "list failed" << endl;
	}
}

bool create(const string &path)
{
	if (!boost::filesystem::is_directory(path)
		&& !boost::filesystem::create_directory(path))
	{
		cerr << "failed to create \"" << path + "\" directory";
		return false;
	}

	return true;
}

int main(int argc, char* argv[])
{
	if (argc < 2)
	{
		help();
	}
	else
	{
		string command(argv[1]);

		if (command == "get")
		{
			if (argc < 3 || argc > 4)
			{
				help();
			}
			else
			{
				string key(argv[2]);
				string path(argc == 3 ? "./" : argv[3]);
				if (create(path)) get(path, key);
			}
		}
		else if (command == "set")
		{
			if (argc < 4 || argc > 5)
			{
				help();
			}
			else
			{
				string key(argv[2]);
				string value(argv[3]);
				string path(argc == 4 ? "./" : argv[4]);
				if (create(path)) set(path, key, value);
			}

		}
		else if (command == "del")
		{
			if (argc < 3 || argc > 4)
			{
				help();
			}
			else
			{
				string key(argv[2]);
				string path(argc == 3 ? "./" : argv[3]);
				if (create(path)) del(path, key);
			}
		}
		else if (command == "list")
		{
			if (argc > 3)
			{
				help();
			}
			else
			{
				string path(argc == 2 ? "./" : argv[2]);
				if (create(path)) listc(path);
			}
		}
		else
		{
			help();
		}
	}

	return 0;
}