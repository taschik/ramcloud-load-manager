#ifndef CONNECTION_H_
#define CONNECTION_H_

#include <string>
#include <iostream>

#include "RamCloud.h"

class Connection {

private:
    string host;
    int port;

    RAMCloud::RamCloud* ramCloud;

public:
    Connection(string host, int port);
    virtual ~Connection();

    void connect();

    string getHost();
    int getPort();

    RAMCloud::RamCloud* getRamCloud();
};

#endif /* CONNECTION_H_ */
