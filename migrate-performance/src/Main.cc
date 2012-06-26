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
using namespace std;
const char * kTableName = "test10";
size_t kNumValues = 100;
Timer myTimer;

int main(int argc, char const *argv[]) {
	const char* coordinatorLocator = "tcp:host=192.168.30.187,port=12246";

	RamCloud cloud(coordinatorLocator);
	debug("[+] Connected\n");

	cloud.createTable(kTableName);
	debug("[+] Created table\n");

	const uint32_t tableId = cloud.getTableId(kTableName);
	debug("[+] Opened table id %u\n", tableId);

	uint64_t version;
	for (unsigned int i = 0; i < 1000; i++) {
		std::ostringstream str;
		str << i;
		std::string key(str.str());
		cloud.write(0, key.c_str(), key.size(), "itemX", 5, NULL, &version);
	}
	printf("writing testdata done\n");

	Context context(true);
	Context::Guard _(context);

	RamCloud client(context, coordinatorLocator);

	Transport::SessionRef session = client.objectFinder.lookup(
			downCast<uint32_t>(tableId), "0", 4);

	MasterClient masterClient(session);
	debug("[+] Migrating data...");
	masterClient.migrateTablet(tableId, 0, ~0UL, RAMCloud::ServerId(2));
	debug("done\n");

    Buffer value;
    int64_t objectValue = 16;

	debug("[+] Writing data for statistics\n");

	masterClient.write(tableId, "key0", 4, &objectValue, 8, NULL, &version);
	masterClient.read(tableId, "key0", 4, &value);
	masterClient.read(tableId, "key0", 4, &value);
	masterClient.read(tableId, "key0", 4, &value);

	ProtoBuf::ServerStatistics serverStats;
	masterClient.getServerStatistics(serverStats);

	cout << "Stats " << serverStats.ShortDebugString() << endl;

	masterClient.splitMasterTablet(0, 0, ~0UL, (~0UL/2));
	masterClient.getServerStatistics(serverStats);
	cout << "Stats " << serverStats.ShortDebugString() << endl;

//	EXPECT_EQ("tabletentry { table_id: 0 "
//			"start_key_hash: 0 "
//			"end_key_hash: 9223372036854775806 } "
//			"tabletentry { table_id: 0 start_key_hash: 9223372036854775807 "
//			"end_key_hash: 18446744073709551615 }",
//			serverStats.ShortDebugString());


	return 0;
}
