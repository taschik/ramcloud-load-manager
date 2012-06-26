/*
 * Migrate.cpp
 *
 *  Created on: May 16, 2012
 *      Author: dtaschik
 */

#include "Migrate.h"
#include "Connection.h"

using namespace RAMCloud;
using namespace std;

Migrate::Migrate(Connection* connection,
		uint64_t tableId,
		uint64_t firstKey,
		uint64_t lastKey,
		uint64_t newOwnerMasterId=0)
{
	this->connection = connection;
	this->tableId = tableId;
	this->firstKey = firstKey;
	this->lastKey = lastKey;
	this->newOwnerMasterId = newOwnerMasterId;
}
Migrate::Migrate(Connection* connection,
		const char* charTableName,
		uint64_t firstKey,
		uint64_t lastKey,
		uint64_t newOwnerMasterId=0)
{
	RamCloud* cloud = connection->getRamCloud();
	uint64_t tableId = cloud->getTableId(charTableName);
	Migrate(connection,tableId, firstKey, lastKey, newOwnerMasterId);
}

Migrate::~Migrate()
{
	// TODO Auto-generated destructor stub
}

void Migrate::migrateTablet()
{
	try {

//		RAMCloud::Context* context;
//		context = new Context(true);

		RAMCloud::Context* context = connection->getContext();
		Context::Guard _(*context);

		string coordinatorLocator = connection->getConnectionString();

		RamCloud client(*context, coordinatorLocator.c_str());

		Transport::SessionRef session = client.objectFinder.lookup(
				downCast<uint32_t>(tableId), reinterpret_cast<char*>(&firstKey),
				sizeof(firstKey));

		MasterClient master(session);

		string currentServiceLocator = session->getServiceLocator();
		if (newOwnerMasterId == 0) {
			newOwnerMasterId = (atoi(&currentServiceLocator[currentServiceLocator.length()-1]) -1 ) %2 +1;
			printf("Migrating to counterpart server in 2 server setup");
		}

		cout << "Issuing migration request:\n" << endl;
		cout << "  table id "<< tableId << endl;
		cout << "  first key " << firstKey << endl;
		cout << "  last key " << lastKey << endl;
		cout << "  current master locator " << currentServiceLocator << endl;
		cout << "  recipient master id " << newOwnerMasterId << endl;

		master.migrateTablet(tableId,
				firstKey,
				lastKey,
				ServerId(newOwnerMasterId));
		cout << "--> Migration complete" << endl;
	}

	catch ( RAMCloud::ServiceLocator::BadServiceLocatorException e){
		cout << "ERROR: migration failed! " << e.what() << endl;
	}
	catch (RAMCloud::RequestFormatError e){
		cout << "ERROR: migration failed! " << e.what() << endl;
	}
	catch (RAMCloud::TableDoesntExistException e) {
		cout << "ERROR: migration failed! " << e.what() << endl;
	}
	catch (RAMCloud::UnknownTableException e){
		cout << "ERROR: migration failed! " << e.what() << endl;
	}
}


