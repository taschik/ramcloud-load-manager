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
    char* connectionString = new char[100];
    sprintf(connectionString, "fast+udp:host=%s,port=%i", host.c_str(), port);
    this->ramCloud = new RamCloud(connectionString);
    

    
    std::cout << "Successfully connected to " << host << std::endl;
}

// void Connection::migrate(string tableName, 
//                         uint64_t tableId, 
//                         uint64_t firstKey, 
//                         uint64_t lastKey, 
//                         uint64_t newOwnerMasterId=0 ){

    // this->context = new Context(true);
    // // this->optionsDescription = new OptionsDescription();
    
    // Context::Guard _(*context);

    // // OptionsDescription migrateOptions("Migrate");
    // // migrateOptions.add_options()
    // //     ("table,t",
    // //      ProgramOptions::value<string>(&tableName)->
    // //         default_value(""),
    // //      "name of the table to migrate.")
    // //     ("firstKey,f",
    // //      ProgramOptions::value<uint64_t>(&firstKey)->
    // //        default_value(0),
    // //      "First key of the tablet range to migrate")
    // //     ("lastKey,z",
    // //      ProgramOptions::value<uint64_t>(&lastKey)->
    // //        default_value(-1),
    // //      "Last key of the tablet range to migrate")
    // //     ("recipient,r",
    // //      ProgramOptions::value<uint64_t>(&newOwnerMasterId)->
    // //        default_value(0),
    // //      "ServerId of the master to migrate to");
    
    // // OptionParser optionParser(migrateOptions, 0, NULL);
    
    // if (tableName == "") {
    //     fprintf(stderr, "error: please specify the table name\n");
    //     exit(1);
    // }
    // if (newOwnerMasterId == 0) {
    //     fprintf(stderr, "error: please specify the recipient's ServerId\n");
    //     exit(1);
    // }
    // string coordinatorLocator = connectionString; //optionParser.options.getCoordinatorLocator();
    // std::cout << "client: Connecting to coordinator " << coordinatorLocator;

    // RamCloud client(*context, coordinatorLocator.c_str());
    // tableId = client.getTableId(tableName.c_str());

    // Transport::SessionRef session = client.objectFinder.lookup(
    //     downCast<uint32_t>(tableId), reinterpret_cast<char*>(&firstKey),
    //     sizeof(firstKey));

    // MasterClient master(session);

    // printf("Issuing migration request:\n");
    // printf("  table \"%s\" (%lu)\n", tableName.c_str(), tableId);
    // printf("  first key %lu\n", firstKey);
    // printf("  last key  %lu\n", lastKey);
    // printf("  current master locator \"%s\"",
    //     session->getServiceLocator().c_str());
    // printf("  recipient master id %lu\n", newOwnerMasterId);

    // master.migrateTablet(tableId,
    //                      firstKey,
    //                      lastKey,
    //                      ServerId(newOwnerMasterId));
// }
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
// Context* Connection::getContext(){
//     return context;
// }
// OptionsDescription* Connection::getOptionsDescription(){
//     return optionsDescription;
// }
// OptionParser* Connection::getOptionParser(){
//     return optionParser;
// }


