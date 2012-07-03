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
#include "ObjectFinder.h"

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
static int intLength(int integer){
	int end = 0;
	while(integer > 0) {
		integer = integer/10;
		end++;
	}
	return end;
}

static char* itoa (int value){

	int length = intLength(value);
	char* key = new char[length];
	sprintf(key,"%d",value);

	return key;
}


int main(int argc, char const *argv[]) {
	const char* coordinatorLocator = "infrc:host=rc30,port=12246";

	RamCloud cloud(coordinatorLocator);
	debug("[+] Connected\n");
	cloud.createTable("tt");
	int tableId = cloud.getTableId("tt");
//	cloud.write(tableId, "key", 3, "value");
	cloud.write(tableId, "key", 3, "value");
	cloud.write(tableId, "key1", 4, "value1");
	cloud.write(tableId, "key1", 4, "value1");

	//
	//		for (int i = 0; i < 60; i++){
	//			string tableId = itoa(i);
	//			cloud.createTable(tableId.c_str());
	//			cout<< "[+] Created table " << i << endl;
	//			cloud.getTableId(tableId.c_str());
	//			cout << "[+] Opened table id " << tableId << endl;
	//
	//			uint64_t version;
	//			for (unsigned int i = 0; i < 1000; i++) {
	//				std::ostringstream str;
	//				str << i;
	//				std::string key(str.str());
	//				cloud.write(0, key.c_str(), key.size(), "itemX", 5, NULL, &version);
	//			}
	//			printf("writing testdata done\n");
	//		}


	Context context(true);
	Context::Guard _(context);

	RamCloud client(context, coordinatorLocator);
	std::set<Transport::SessionRef> sessions;
	sessions = client.objectFinder.tableLookup(tableId);

	//	Transport::SessionRef session = client.objectFinder.lookup(
	//			downCast<uint32_t>(tableId), "0", 4);
	std::set<Transport::SessionRef>::iterator it;
	Transport::SessionRef session;
	for (it=sessions.begin(); it != sessions.end(); it++){
		session = *it;
	}
	MasterClient masterClient(session);
	debug("[+] Migrating data...");

	masterClient.migrateTablet(tableId, 0, ~0UL, RAMCloud::ServerId(2));
	debug("done\n");

	return 0;
}

