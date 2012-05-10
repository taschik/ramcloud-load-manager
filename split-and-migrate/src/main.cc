#include "Connection.h"

#include <iostream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <exception>

using namespace std;
using namespace RAMCloud;

/**
 * GLOBALS
 */
Connection* connection = NULL;
uint32_t INSERTAMOUNT = 20;
string host = "192.168.30.187";
int port = 12246;

void split(RAMCloud::RamCloud* cloud, const char* tableName){
    try {
       cloud->splitTablet(tableName, 0, ~0UL, (~0UL/2));
       std::cout << "Split Successfully" << std::endl;
    }
    catch(RAMCloud::TabletDoesntExistException e){

        std::cout << "ERROR: split failed: " << e.what() << std::endl;
    }
}

void migrate(Connection* connection,
            const char* charTableName,
            uint64_t tableId, 
            uint64_t firstKey, 
            uint64_t lastKey, 
            uint64_t newOwnerMasterId=0){
try {
    string tableName = charTableName;


    RAMCloud::Context* context;
    context = new Context(true);
    
    Context::Guard _(*context);
    
    if (tableName == "") {
        fprintf(stderr, "error: please specify the table name\n");
        exit(1);
    }
    if (newOwnerMasterId == 0) {
        fprintf(stderr, "error: please specify the recipient's ServerId\n");
        exit(1);
    }
    string coordinatorLocator = connection->getConnectionString();
    std::cout << "client: Connecting to coordinator " << coordinatorLocator << endl;

    RamCloud client(*context, coordinatorLocator.c_str());
    tableId = client.getTableId(tableName.c_str());
    
    Transport::SessionRef session = client.objectFinder.lookup(
        downCast<uint32_t>(tableId), reinterpret_cast<char*>(&firstKey),
        sizeof(firstKey));

    MasterClient master(session);

    printf("Issuing migration request:\n");
    printf("  table \"%s\" (%lu)\n", tableName.c_str(), tableId);
    printf("  first key %lu\n", firstKey);
    printf("  last key  %lu\n", lastKey);
    printf("  current master locator \"%s\"\n", session->getServiceLocator().c_str());
    printf("  recipient master id %lu\n", newOwnerMasterId);

    master.migrateTablet(tableId,
                         firstKey,
                         lastKey,
                         ServerId(newOwnerMasterId));
    cout << "Migration complete" << endl;
    }
    catch ( RAMCloud::ServiceLocator::BadServiceLocatorException e){
         std::cout << "ERROR: migration failed! " << e.what() << std::endl;
    }
    catch (RAMCloud::RequestFormatError e){
        std::cout << "ERROR: migration failed! " << e.what() << std::endl;
    }
}

int main(int argc, char const *argv[])
{

    connection = new Connection(host, port);
    connection->connect();


    RAMCloud::RamCloud* cloud = connection->getRamCloud();
    const char* tableName = "split";

    try {
         if (cloud->getTableId(tableName)){
            cloud->dropTable(tableName);
        }
    }
    catch (RAMCloud::TableDoesntExistException e){
        cout << "Table does not exist! I am going to create it!" << endl;
    }

    cloud->createTable(tableName,0);
    const uint64_t tableId = cloud->getTableId(tableName);

    // write INSERTAMOUNT Values to RAMCloud
    for (unsigned int i=0; i < INSERTAMOUNT; i++)
    {   
        
        char* key = Helper::itoa(i);
        // std::cout << key << " size:" << strlen(key) << std::endl;
        
        char* value = new char[strlen(key)+strlen("_Hallo")]; 
        sprintf(value, "%d_Hallo", i);

        cloud->write(tableId, key, strlen(key), value);
    }
    
    // split(cloud, tableName);
    migrate(connection, tableName, tableId, 0, ~0UL, 1);

    //read INSERTAMOUNT Values from RAMCloud
    for (unsigned int i=0; i < INSERTAMOUNT; i++)
    {   
        RAMCloud::Buffer buffer;

        char* key = Helper::itoa(i);

        cloud->read(tableId, key, strlen(key), &buffer);

        uint16_t length = buffer.getTotalLength();

        char* str = new char[length + 1];
        str[length] = 0;
        
        buffer.copy(0, buffer.getTotalLength(), str);
        std::cout << "Key: " << key << " Value: " << str << std::endl;
    }

    
    return 0;
}
