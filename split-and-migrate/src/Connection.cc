#include "Connection.h"

Connection::Connection(string host, int port) {
    this->host = host;
    this->port = port;
}

Connection::~Connection() {
    // TODO Auto-generated destructor stub
}


void Connection::connect() {
    char* connectionString = new char[100];
    sprintf(connectionString, "fast+udp:host=%s,port=%i", host.c_str(), port);
    std::cout << "Connection String " << connectionString << std::endl;
    this->ramCloud = new RAMCloud::RamCloud(connectionString);
}

string Connection::getHost() {
    return host;
}

int Connection::getPort() {
    return port;
}

RAMCloud::RamCloud* Connection::getRamCloud() {
    return ramCloud;
}


