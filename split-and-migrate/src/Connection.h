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
    // RAMCloud::Context* context;
    // RAMCloud::OptionsDescription* optionsDescription;
    // RAMCloud::OptionParser* optionParser;

public:
    Connection(string host, int port);
    virtual ~Connection();

    void connect();

    string getHost();
    string getConnectionString();
    int getPort();

    RAMCloud::RamCloud* getRamCloud();

};

#endif /* CONNECTION_H_ */
