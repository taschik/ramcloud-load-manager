#include "RamCloud.h"

#include <iostream>
#include <string>


using namespace std;

int main(int argc, char const *argv[])
{

    RAMCloud::RamCloud cloud("fast+udp:host=127.0.0.1,port=12246");
    
    const char* kTableName = "hello_world_table";

    cloud.createTable(kTableName);

    const uint64_t tableId = cloud.getTableId(kTableName);

    //write to RAMCloud, key "0" (which has the keylength 1) has the value HalloHPI
    cloud.write(tableId, "0", 1, "Hallo Christian");

    RAMCloud::Buffer buffer;

    cloud.read(tableId, "0", 1, &buffer);

    uint16_t length = buffer.getTotalLength();

    char* str = new char[length];

    uint32_t returned = buffer.copy(0, length, str);


    std::cout << "Greetings from RAMCloud: " << str << std::endl;
    
    return 0;
}