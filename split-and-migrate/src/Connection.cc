#include "Connection.h"

using namespace RAMCloud;



Connection::Connection(string connectionString) {
    this->connectionString = connectionString;
    std::cout << "Connection String " << connectionString << std::endl;

}

Connection::~Connection() {
    // TODO Auto-generated destructor stub
}


void Connection::connect() {
	this->context = new Context(true);

    this->ramCloud = new RamCloud(*context, connectionString.c_str());

    std::cout << "Successfully connected to " << connectionString << std::endl;
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
Context* Connection::getContext(){
	return context;
}
//MasterClient* Connection::getMasterClient(unsigned int tableId, std::string object){
//
//	RamCloud* ramCloud = getRamCloud();
//
//
//	return ramCloud->;
//}
unsigned int Connection::getTableIdFromName(string name){
	return ramCloud->getTableId(name.c_str());
}
unsigned int Connection::getTableId()
{
	return this->tableId;
}
void Connection::setTableName(string &tableName) {
	this->tableName = tableName;
	this->tableId = ramCloud->getTableId(tableName.c_str());
}

string Connection::getTableName(){
	return tableName;
}

void Connection::unsetTableName(){
	this->tableName = "";
	this->tableId = -1;
}

