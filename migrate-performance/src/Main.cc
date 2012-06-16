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

	cloud.createTable(kTableName);
	debug("[+] Created table\n");

	const uint32_t tableId = cloud.getTableId(kTableName);
	debug("[+] Opened table id %u\n", tableId);

	uint64_t version;
	for (unsigned int i = 0; i < 30; i++) {
		std::ostringstream str;
		str << i;
		std::string key(str.str());
		cloud.write(0, key.c_str(), key.size(), "itemX", 5, NULL, &version);
	}
	printf("writing testdata done\n");

	Context context(true);
	Context::Guard _(context);

	std::string coordinatorLocator = "fast+udp:host=0.0.0.0,port=30000";
	RamCloud client(context, coordinatorLocator.c_str());

	Transport::SessionRef session = client.objectFinder.lookup(
			downCast<uint32_t>(tableId), "0", 4);

	MasterClient master(session);
	master.migrateTablet(tableId, 0, ~0UL, RAMCloud::ServerId(2));

	//Transport::SessionRef session2 = client.objectFinder.lookup(
	//		  downCast<uint32_t>(tableId), "key0", 4);
	//
	//std::cout << "mjam" << std::endl;
	//std::string test = static_cast<BindTransport::BindSession*>(session2.get())->locator;
	//std::cout << test << std::endl;

	return 0;
}
