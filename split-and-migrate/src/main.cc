#include "Connection.h"

#include <iostream>
#include <string>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

/**
 * GLOBALS
 */
Connection* connection = NULL;
uint32_t INSERTAMOUNT = 15;
string host = "192.168.30.187";
int port = 12246;

int intLength(int integer){
    int end = 0;
     while(integer > 0) {
                integer = integer/10;
                end++;
        }
    return end;
}

char* itoa (int value){

    int length = intLength(value);
    char* key = new char[length];
    sprintf(key,"%d",value);

    return key;
}

int main(int argc, char const *argv[])
{

    connection = new Connection(host, port);
    connection->connect();

    std::cout << "Successfully connected to " << host << std::endl;

    RAMCloud::RamCloud* cloud = connection->getRamCloud();

    const char* kTableName = "split";

    cloud->createTable(kTableName);

    const uint64_t tableId = cloud->getTableId(kTableName);


    //write INSERTAMOUNT Values to RAMCloud
    for (unsigned int i=0; i < INSERTAMOUNT; i++)
    {   
        
        char* key = itoa(i);
        //std::cout << key << " size:" << strlen(key) << std::endl;
        
        char* value = new char[strlen(key)+strlen("_Hallo")]; 
        sprintf(value, "%d_Hallo", i);
        

        cloud->write(tableId, key, strlen(key), value);
    }

    //read INSERTAMOUNT Values from RAMCloud
    for (unsigned int i=0; i < INSERTAMOUNT; i++)
    {   
        RAMCloud::Buffer buffer;


        char* key = itoa(i);

        cloud->read(tableId, key, strlen(key), &buffer);
        uint16_t length = buffer.getTotalLength();
        char* str = new char[length];

        buffer.copy(0, buffer.getTotalLength(), str);
        std::cout << "Key: " << key << " Value: " << str << std::endl;
    }

    
    return 0;
}
