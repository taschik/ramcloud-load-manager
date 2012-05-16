#include "global.h"

class Split {

private:
    RAMCloud::RamCloud* cloud;

	const char* tableName;
    uint64_t startKeyHash;
    uint64_t endKeyHash;
    uint64_t splitKeyHash;

public:
    Split(RAMCloud::RamCloud* cloud,
    		const char* tableName,
    		uint64_t startKeyHash,
    		uint64_t endKeyHash,
    		uint64_t splitKeyHash);
    virtual ~Split();

    void splitTable();
};
