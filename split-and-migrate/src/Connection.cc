#include "Connection.h"

using namespace RAMCloud;



Connection::Connection(string host, int port) {
    this->host = host;
    this->port = port;
    this->connectionString = "fast+udp:host=" + host + ",port=" + Helper::itoa(port);
    std::cout << "Connection String " << connectionString << std::endl;

}

Connection::~Connection() {
    // TODO Auto-generated destructor stub
}


void Connection::connect() {

    this->ramCloud = new RamCloud(connectionString.c_str());
    std::cout << "Successfully connected to " << host << std::endl;
}


string Connection::getHost() {
    return host;
}
string Connection::getConnectionString() {
    return connectionString;
}

int Connection::getPort() {
    return port;
}

RamCloud* Connection::getRamCloud() {
    return ramCloud;
}

unsigned int Connection::getTableId()
{
	return tableId;
}
void Connection::setTableName(string &tableName) {
	tableName = tableName;
	tableId = ramCloud->getTableId(tableName.c_str());
}

string Connection::getTableName(){
	return tableName;
}

void Connection::unsetTableName(){
	this->tableName = "";
	this->tableId = -1;
}

