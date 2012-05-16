/*
 * Migrate.h
 *
 *  Created on: May 16, 2012
 *      Author: dtaschik
 */

#ifndef MIGRATE_H_
#define MIGRATE_H_

#include "Connection.h"
#include "global.h"

class Migrate
{
private:
	Connection* connection;
	const char* charTableName;
	uint64_t tableId;
	uint64_t firstKey;
	uint64_t lastKey;
	uint64_t newOwnerMasterId;

public:
	Migrate(Connection* connection,
			const char* charTableName,
			uint64_t firstKey,
			uint64_t lastKey,
			uint64_t newOwnerMasterId);
	Migrate(Connection* connection,
			uint64_t tableId,
			uint64_t firstKey,
			uint64_t lastKey,
			uint64_t newOwnerMasterId);
	virtual ~Migrate();

	void migrateTablet();
};

#endif /* MIGRATE_H_ */
