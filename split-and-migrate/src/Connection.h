#ifndef CONNECTION_H_
#define CONNECTION_H_

#include <string>
#include <iostream>

#include "RamCloud.h"
#include "MasterClient.h"
#include "Context.h"
#include "OptionParser.h"
#include "Common.h"
#include "Helper.h"

class Connection {

private:
	string host;
	string connectionString;
	int port;

	RAMCloud::RamCloud* ramCloud;
	RAMCloud::Context* context;
	string tableName;
	unsigned int tableId;


public:
	Connection(string host, int port);
	virtual ~Connection();

	void connect();

	string getHost();
	string getConnectionString();
	int getPort();

	unsigned int getTableId();
	unsigned int getTableIdFromName(string name);

	string getTableName();
	void setTableName(string &tableName);
	void unsetTableName();

	RAMCloud::RamCloud* getRamCloud();
	RAMCloud::Context* getContext();
	//    RAMCloud::MasterClient* getMasterClient(unsigned int tableId, std::string object);

};

#endif /* CONNECTION_H_ */
