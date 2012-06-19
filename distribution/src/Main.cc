#include "Buffer.h"
#include "Common.h"
#include "RamCloud.h"
#include "KeyUtil.h"
#include "Context.h"
#include "MasterClient.h"
#include "BindTransport.h"
#include "Transport.h"
#include "RejectRules.h"
#include "Timer.h"

#include <sys/time.h>
#include <unistd.h>
#include <cassert>
#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <stdint.h>
#include <string>
#include <iostream>

#if 1
#define debug(...) fprintf(stderr, __VA_ARGS__)
#else
#define debug(...) {}
#endif

using namespace RAMCloud;

const char * kTableName = "test";
size_t kNumValues = 100;
Timer myTimer;

int main(int argc, char const *argv[]) {
	RamCloud cloud("fast+udp:host=0.0.0.0,port=30000");
	debug("[+] Connected\n");

	cloud.createTable(kTableName, 2);
	debug("[+] Created table\n");

	const uint32_t tableId = cloud.getTableId(kTableName);
	debug("[+] Opened table id %u\n", tableId);

	uint64_t version;
	for (unsigned int i = 0; i < 200; i++) {
		std::ostringstream str;
		str << i;
		std::string key(str.str());
		cloud.write(tableId, key.c_str(), key.size(), "itemX", 5, NULL, &version);
	}
	debug("writing testdata done\n");

	Context context(true);
	Context::Guard _(context);

	std::string coordinatorLocator = "fast+udp:host=0.0.0.0,port=30000";
	RamCloud client(context, coordinatorLocator.c_str());
	CoordinatorClient coordinatorClient(coordinatorLocator.c_str());
	ObjectFinder objectFinder(coordinatorClient);
	std::vector<ObjectFinder::KeysAtServer> keys = objectFinder.resolveTableDistribution(tableId, 1000);

	for (uint32_t i = 0; i < keys.size(); i++) {
		std::cout << "i: " << i << std::endl;
		for (uint32_t j = 0; i < keys[j].keys.size(); j++) {
			std::cout << keys[i].keys[0] << std::endl;
		}
	}

	cloud.dropTable(kTableName);

	return 0;
}
