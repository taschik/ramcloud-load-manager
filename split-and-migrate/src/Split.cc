#include "Split.h"

using namespace std;

Split::Split(RAMCloud::RamCloud *cloud,
		const char* tableName,
		uint64_t startKeyHash,
		uint64_t endKeyHash,
		uint64_t splitKeyHash)
{
    this->cloud = cloud;
    this->tableName = tableName;
    this->startKeyHash = startKeyHash;
    this->endKeyHash = endKeyHash;
    this->splitKeyHash = splitKeyHash;
}

Split::~Split() {
    // TODO Auto-generated destructor stub
}

void Split::splitTable(){
    try {
       cloud->splitTablet(tableName, startKeyHash, endKeyHash, splitKeyHash);
       cout << "--> Split Successfully!" << endl;
    }
    catch(RAMCloud::TabletDoesntExistException e){
        cout << "ERROR: split of "<< tableName << " failed: " << e.what() << endl;
    }
	catch (RAMCloud::RequestFormatError e){
		cout << "ERROR: split failed! " << e.what() << endl;
	}
}
