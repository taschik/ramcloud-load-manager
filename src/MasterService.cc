/* Copyright (c) 2009-2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <unordered_map>
#include <unordered_set>

#include "Buffer.h"
#include "ClientException.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "LogIterator.h"
#include "ShortMacros.h"
#include "MasterClient.h"
#include "MasterService.h"
#include "RawMetrics.h"
#include "Tub.h"
#include "ProtoBuf.h"
#include "Rpc.h"
#include "Segment.h"
#include "ServiceManager.h"
#include "Transport.h"
#include "WallTime.h"

namespace RAMCloud {

// struct MasterService::Replica

/**
 * Constructor.
 * \param backupId
 *      See #backupId member.
 * \param segmentId
 *      See #segmentId member.
 * \param state
 *      See #state member. The default (NOT_STARTED) is usually what you want
 *      here, but other values are allowed for testing.
 */
MasterService::Replica::Replica(uint64_t backupId, uint64_t segmentId,
                                State state)
    : backupId(backupId)
    , segmentId(segmentId)
    , state(state)
{
}

// --- MasterService ---

bool objectLivenessCallback(LogEntryHandle handle,
                            void* cookie);
bool objectRelocationCallback(LogEntryHandle oldHandle,
                              LogEntryHandle newHandle,
                              void* cookie);
uint32_t objectTimestampCallback(LogEntryHandle handle);

bool tombstoneLivenessCallback(LogEntryHandle handle,
                               void* cookie);
bool tombstoneRelocationCallback(LogEntryHandle oldHandle,
                                 LogEntryHandle newHandle,
                                 void* cookie);
uint32_t tombstoneTimestampCallback(LogEntryHandle handle);

/**
 * Construct a MasterService.
 *
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 * \param coordinator
 *      A client to the coordinator for the RAMCloud this Master is in.
 * \param serverList
 *      A reference to the global ServerList.
 */
MasterService::MasterService(const ServerConfig& config,
                             CoordinatorClient* coordinator,
                             ServerList& serverList)
    : config(config)
    , coordinator(coordinator)
    , serverId()
    , serverList(serverList)
    , replicaManager(serverList, serverId,
                     config.master.numReplicas, &config.coordinatorLocator)
    , bytesWritten(0)
    , log(serverId,
          config.master.logBytes,
          config.segmentSize,
          downCast<uint32_t>(sizeof(Object)) +
                config.maxObjectKeySize +
                config.maxObjectDataSize,
          &replicaManager,
          config.master.disableLogCleaner ? Log::CLEANER_DISABLED :
                                            Log::CONCURRENT_CLEANER)
    , objectMap(config.master.hashTableBytes /
        HashTable<LogEntryHandle>::bytesPerCacheLine())
    , tablets()
    , initCalled(false)
    , anyWrites(false)
    , objectUpdateLock()
{
    log.registerType(LOG_ENTRY_TYPE_OBJ,
                     true,
                     objectLivenessCallback,
                     this,
                     objectRelocationCallback,
                     this,
                     objectTimestampCallback);
    log.registerType(LOG_ENTRY_TYPE_OBJTOMB,
                     false,
                     tombstoneLivenessCallback,
                     this,
                     tombstoneRelocationCallback,
                     this,
                     tombstoneTimestampCallback);

    replicaManager.startFailureMonitor(&log);
}

MasterService::~MasterService()
{
    replicaManager.haltFailureMonitor();
    std::set<Table*> tables;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
        tables.insert(reinterpret_cast<Table*>(tablet.user_data()));
    foreach (Table* table, tables)
        delete table;
}

void
MasterService::dispatch(RpcOpcode opcode, Rpc& rpc)
{
    assert(initCalled);

    std::lock_guard<SpinLock> lock(objectUpdateLock);

    switch (opcode) {
        case DropTabletOwnershipRpc::opcode:
            callHandler<DropTabletOwnershipRpc, MasterService,
                        &MasterService::dropTabletOwnership>(rpc);
            break;
        case FillWithTestDataRpc::opcode:
            callHandler<FillWithTestDataRpc, MasterService,
                        &MasterService::fillWithTestData>(rpc);
            break;
        case IncrementRpc::opcode:
            callHandler<IncrementRpc, MasterService,
                        &MasterService::increment>(rpc);
            break;
        case IsReplicaNeededRpc::opcode:
            callHandler<IsReplicaNeededRpc, MasterService,
                        &MasterService::isReplicaNeeded>(rpc);
            break;
        case GetServerStatisticsRpc::opcode:
            callHandler<GetServerStatisticsRpc, MasterService,
                        &MasterService::getServerStatistics>(rpc);
            break;
        case GetHeadOfLogRpc::opcode:
            callHandler<GetHeadOfLogRpc, MasterService,
                        &MasterService::getHeadOfLog>(rpc);
            break;
        case MigrateTabletRpc::opcode:
            callHandler<MigrateTabletRpc, MasterService,
                        &MasterService::migrateTablet>(rpc);
            break;
        case MultiReadRpc::opcode:
            callHandler<MultiReadRpc, MasterService,
                        &MasterService::multiRead>(rpc);
            break;
        case PrepForMigrationRpc::opcode:
            callHandler<PrepForMigrationRpc, MasterService,
                        &MasterService::prepForMigration>(rpc);
            break;
        case ReadRpc::opcode:
            callHandler<ReadRpc, MasterService,
                        &MasterService::read>(rpc);
            break;
        case ReceiveMigrationDataRpc::opcode:
            callHandler<ReceiveMigrationDataRpc, MasterService,
                        &MasterService::receiveMigrationData>(rpc);
            break;
        case RecoverRpc::opcode:
            callHandler<RecoverRpc, MasterService,
                        &MasterService::recover>(rpc);
            break;
        case RemoveRpc::opcode:
            callHandler<RemoveRpc, MasterService,
                        &MasterService::remove>(rpc);
            break;
        case SplitMasterTabletRpc::opcode:
            callHandler<SplitMasterTabletRpc, MasterService,
                        &MasterService::splitMasterTablet>(rpc);
            break;
        case TakeTabletOwnershipRpc::opcode:
            callHandler<TakeTabletOwnershipRpc, MasterService,
                        &MasterService::takeTabletOwnership>(rpc);
            break;
        case WriteRpc::opcode:
            callHandler<WriteRpc, MasterService,
                        &MasterService::write>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}


/**
 * Perform once-only initialization for the master service after having
 * enlisted the process with the coordinator.
 */
void
MasterService::init(ServerId id)
{
    assert(!initCalled);

    serverId = id;
    LOG(NOTICE, "My server ID is %lu", serverId.getId());
    metrics->serverId = serverId.getId();

    initCalled = true;
}

/**
 * Fill this server with test data. Objects are added to all
 * existing tables in a round-robin fashion.
 * \copydetails Service::ping
 */
void
MasterService::fillWithTestData(const FillWithTestDataRpc::Request& reqHdr,
                                FillWithTestDataRpc::Response& respHdr,
                                Rpc& rpc)
{
    LOG(NOTICE, "Filling with %u objects of %u bytes each in %u tablets",
        reqHdr.numObjects, reqHdr.objectSize, tablets.tablet_size());

    Table* tables[tablets.tablet_size()];
    uint32_t i = 0;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
        tables[i++] = reinterpret_cast<Table*>(tablet.user_data());

    RejectRules rejectRules;
    memset(&rejectRules, 0, sizeof(RejectRules));
    rejectRules.exists = 1;

    for (uint32_t objects = 0; objects < reqHdr.numObjects; objects++) {
        Buffer buffer;

        int t = objects % tablets.tablet_size();

        string keyString = format("%d", objects / tablets.tablet_size());
        uint16_t keyLength = static_cast<uint16_t>(keyString.length());

        char* keyLocation = new(&buffer, APPEND) char[keyLength];
        memcpy(keyLocation, keyString.c_str(), keyLength);

        // safe? doubtful. simple? you bet.
        char data[reqHdr.objectSize];
        memset(data, 0xcc, reqHdr.objectSize);
        Buffer::Chunk::appendToBuffer(&buffer, data, reqHdr.objectSize);

        uint64_t newVersion;
        Status status = storeData(tables[t]->getId(), &rejectRules, &buffer,
                                  0, keyLength, reqHdr.objectSize,
                                  &newVersion, true);
        if (status != STATUS_OK) {
            respHdr.common.status = status;
            return;
        }
        if ((objects % 50) == 0) {
            replicaManager.proceed();
        }
    }

    log.sync();

    LOG(NOTICE, "Done writing objects.");
}

/**
 * Top-level server method to handle the GET_HEAD_OF_LOG request.
 */
void
MasterService::getHeadOfLog(const GetHeadOfLogRpc::Request& reqHdr,
                            GetHeadOfLogRpc::Response& respHdr,
                            Rpc& rpc)
{
    LogPosition head = log.headOfLog();
    respHdr.headSegmentId = head.segmentId();
    respHdr.headSegmentOffset = head.segmentOffset();
}

/**
 * Top-level server method to handle the GET_SERVER_STATISTICS request.
 */
void
MasterService::getServerStatistics(
                            const GetServerStatisticsRpc::Request& reqHdr,
                            GetServerStatisticsRpc::Response& respHdr,
                            Rpc& rpc)
{
    ProtoBuf::ServerStatistics serverStats;

    foreach (const ProtoBuf::Tablets::Tablet& i, tablets.tablet()) {
        Table* table = reinterpret_cast<Table*>(i.user_data());
        *serverStats.add_tabletentry() = table->statEntry;
    }

    respHdr.serverStatsLength = serializeToResponse(rpc.replyPayload,
                                                    serverStats);
}

/**
 * Top-level server method to handle the MULTIREAD request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains the parameters
 *      for this operation except the tableId, key, keyLength for each
 *      of the objects to be read.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call.
 *      It contains the tableId, key and keyLength for each of the
 *      objects to be read. It can also be used to read additional
 *      information beyond the request header and/or append additional
 *      information to the response buffer.
 */
void
MasterService::multiRead(const MultiReadRpc::Request& reqHdr,
                         MultiReadRpc::Response& respHdr,
                         Rpc& rpc)
{
    uint32_t numRequests = reqHdr.count;
    uint32_t reqOffset = downCast<uint32_t>(sizeof(reqHdr));

    respHdr.count = numRequests;

    // Each iteration extracts one request from request rpc, finds the
    // corresponding object, and appends the response to the response rpc.
    for (uint32_t i = 0; i < numRequests; i++) {
        const MultiReadRpc::Request::Part *currentReq =
            rpc.requestPayload.getOffset<MultiReadRpc::Request::Part>(
            reqOffset);
        reqOffset += downCast<uint32_t>(sizeof(MultiReadRpc::Request::Part));
        const char* key =
            static_cast<const char*>(rpc.requestPayload.getRange(
            reqOffset, currentReq->keyLength));
        reqOffset += downCast<uint32_t>(currentReq->keyLength);

        Status* status = new(&rpc.replyPayload, APPEND) Status(STATUS_OK);
        // We must note the status if the table does not exist. Also, we might
        // have an entry in the hash table that's invalid because its tablet no
        // longer lives here.
        if (getTable(currentReq->tableId, key, currentReq->keyLength) == NULL) {
            *status = STATUS_UNKNOWN_TABLE;
            continue;
        }
        LogEntryHandle handle = objectMap.lookup(currentReq->tableId,
                                                 key, currentReq->keyLength);
        if (handle == NULL || handle->type() != LOG_ENTRY_TYPE_OBJ) {
             *status = STATUS_OBJECT_DOESNT_EXIST;
             continue;
        }

        const SegmentEntry* entry =
            reinterpret_cast<const SegmentEntry*>(handle);
        Buffer::Chunk::appendToBuffer(&rpc.replyPayload, entry,
            downCast<uint32_t>(sizeof(SegmentEntry)) + handle->length());

    }
}

/**
 * Top-level server method to handle the READ request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains all the
 *      parameters for this operation except the key of the object.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call.
 *      It contains the key for the object. It can also be used to
 *      read additional information beyond the request header and/or
 *      append additional information to the response buffer.
 */
void
MasterService::read(const ReadRpc::Request& reqHdr,
                    ReadRpc::Response& respHdr,
                    Rpc& rpc)
{
    uint32_t reqOffset = downCast<uint32_t>(sizeof(reqHdr));
    const char* key = static_cast<const char*>(rpc.requestPayload.getRange(
                                               reqOffset, reqHdr.keyLength));

    // We must return table doesn't exist if the table does not exist. Also, we
    // might have an entry in the hash table that's invalid because its tablet
    // no longer lives here.

    if (getTable(reqHdr.tableId, key, reqHdr.keyLength) == NULL) {
        respHdr.common.status = STATUS_UNKNOWN_TABLE;
        return;
    }

    LogEntryHandle handle = objectMap.lookup(reqHdr.tableId,
                                             key, reqHdr.keyLength);

    if (handle == NULL || handle->type() != LOG_ENTRY_TYPE_OBJ) {
        respHdr.common.status = STATUS_OBJECT_DOESNT_EXIST;
        return;
    }

    const Object* obj = handle->userData<Object>();
    respHdr.version = obj->version;
    Status status = rejectOperation(reqHdr.rejectRules, obj->version);
    if (status != STATUS_OK) {
        respHdr.common.status = status;
        return;
    }

    Buffer::Chunk::appendToBuffer(&rpc.replyPayload,
        obj->getData(), obj->dataLength(handle->length()));
    // TODO(ongaro): We'll need a new type of Chunk to block the cleaner
    // from scribbling over obj->data.
    respHdr.length = obj->dataLength(handle->length());
}

/**
 * Top-level server method to handle the DROP_TABLET_OWNERSHIP request.
 *
 * This RPC is issued by the coordinator when a table is dropped and all
 * tablets are being destroyed. This is not currently used in migration,
 * since the source master knows that it no longer owns the tablet when
 * the coordinator has responded to its REASSIGN_TABLET_OWNERSHIP rpc.
 *
 * \copydetails Service::ping
 */
void
MasterService::dropTabletOwnership(
    const DropTabletOwnershipRpc::Request& reqHdr,
    DropTabletOwnershipRpc::Response& respHdr,
    Rpc& rpc)
{
    int index = 0;
    foreach (ProtoBuf::Tablets::Tablet& i, *tablets.mutable_tablet()) {
        if (reqHdr.tableId == i.table_id() &&
          reqHdr.firstKey == i.start_key_hash() &&
          reqHdr.lastKey == i.end_key_hash()) {
            LOG(NOTICE, "Dropping ownership of tablet (%lu, range [%lu,%lu])",
                reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);
            Table* table = reinterpret_cast<Table*>(i.user_data());
            delete table;
            tablets.mutable_tablet()->SwapElements(
                tablets.tablet_size() - 1, index);
            tablets.mutable_tablet()->RemoveLast();
            return;
        }

        index++;
    }

    LOG(WARNING, "Could not drop ownership on unknown tablet (%lu, range "
        "[%lu,%lu])!", reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);
    respHdr.common.status = STATUS_UNKNOWN_TABLE;
}

/**
 * Top-level server method to handle the SPLIT_MASTER_TABLET_OWNERSHIP request.
 *
 * This RPC is issued by the coordinator when a tablet should be splitted. The
 * coordinator specifies the to be splitted tablet and at which point the split
 * should occur (splitKeyHash).
 *
 * \copydetails Service::ping
 */
void
MasterService::splitMasterTablet(
    const SplitMasterTabletRpc::Request& reqHdr,
    SplitMasterTabletRpc::Response& respHdr,
    Rpc& rpc)
{
    ProtoBuf::Tablets_Tablet newTablet;

    foreach (ProtoBuf::Tablets::Tablet& i, *tablets.mutable_tablet()) {
        if (reqHdr.tableId == i.table_id() &&
          reqHdr.startKeyHash == i.start_key_hash() &&
          reqHdr.endKeyHash == i.end_key_hash()) {

            newTablet = i;

            Table* newTable = new Table(reqHdr.tableId, reqHdr.startKeyHash,
                                 reqHdr.splitKeyHash - 1);
            i.set_user_data(reinterpret_cast<uint64_t>(newTable));
            i.set_end_key_hash(reqHdr.splitKeyHash - 1);
        }

    }

    newTablet.set_start_key_hash(reqHdr.splitKeyHash);
    Table* newTable = new Table(reqHdr.tableId, reqHdr.splitKeyHash,
                                 reqHdr.endKeyHash);
    newTablet.set_user_data(reinterpret_cast<uint64_t>(newTable));

    *tablets.add_tablet() = newTablet;

    LOG(NOTICE, "In table '%lu' I split the tablet that started at key %lu and "
                "ended at key %lu", reqHdr.tableId, reqHdr.startKeyHash,
                reqHdr.endKeyHash);
}

/**
 * Top-level server method to handle the TAKE_TABLET_OWNERSHIP request.
 *
 * This RPC is issued by the coordinator when assigning ownership of a
 * tablet. This can occur due to both tablet creation and to complete
 * migration. As far as the coordinator is concerned, the master
 * receiving this rpc owns the tablet specified and all requests for it
 * will be directed here from now on.
 *
 * \copydetails Service::ping
 */
void
MasterService::takeTabletOwnership(
    const TakeTabletOwnershipRpc::Request& reqHdr,
    TakeTabletOwnershipRpc::Response& respHdr,
    Rpc& rpc)
{
    if (log.headOfLog() == LogPosition()) {
        // Before any tablets can be assigned to this master it must have at
        // least one segment on backups, otherwise it is impossible to
        // distinguish between the loss of its entire log and the case where no
        // data was ever written to it.
        LOG(DEBUG, "Allocating log head before accepting tablet assignment");
        log.allocateHead();
        log.sync();
    }

    ProtoBuf::Tablets::Tablet* tablet = NULL;
    foreach (ProtoBuf::Tablets::Tablet& i, *tablets.mutable_tablet()) {
        if (reqHdr.tableId == i.table_id() &&
          reqHdr.firstKey == i.start_key_hash() &&
          reqHdr.lastKey == i.end_key_hash()) {
            tablet = &i;
            break;
        }
    }

    if (tablet == NULL) {
        // Sanity check that this tablet doesn't overlap with an existing one.
        if (getTableForHash(reqHdr.tableId, reqHdr.firstKey) != NULL ||
          getTableForHash(reqHdr.tableId, reqHdr.lastKey) != NULL) {
            LOG(WARNING, "Tablet being assigned (%lu, range [%lu,%lu]) "
                "partially overlaps an existing tablet!", reqHdr.tableId,
                reqHdr.firstKey, reqHdr.lastKey);
            // TODO(anybody): Do we want a more meaningful error code?
            respHdr.common.status = STATUS_INTERNAL_ERROR;
            return;
        }

        LOG(NOTICE, "Taking ownership of new tablet (%lu, range "
            "[%lu,%lu])", reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);


        ProtoBuf::Tablets_Tablet& newTablet(*tablets.add_tablet());
        newTablet.set_table_id(reqHdr.tableId);
        newTablet.set_start_key_hash(reqHdr.firstKey);
        newTablet.set_end_key_hash(reqHdr.lastKey);
        newTablet.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);

        Table* table = new Table(reqHdr.tableId, reqHdr.firstKey,
                                 reqHdr.lastKey);
        newTablet.set_user_data(reinterpret_cast<uint64_t>(table));
    } else {
        LOG(NOTICE, "Taking ownership of existing tablet (%lu, range "
            "[%lu,%lu]) in state %d", reqHdr.tableId, reqHdr.firstKey,
            reqHdr.lastKey, tablet->state());

        if (tablet->state() != ProtoBuf::Tablets_Tablet_State_RECOVERING) {
            LOG(WARNING, "Taking ownership when existing tablet is in "
                "unexpected state (%d)!", tablet->state());
        }

        tablet->set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);

        // If we took ownership after migration, then recoverSegment() may have
        // added tombstones to the hash table. Clean them up.
        removeTombstones();
    }
}

/**
 * Top-level server method to handle the PREP_FOR_MIGRATION request.
 *
 * This is used during tablet migration to request that a destination
 * master take on a tablet from the current owner. The receiver may
 * accept or refuse.
 *
 * \copydetails Service::ping
 */
void
MasterService::prepForMigration(const PrepForMigrationRpc::Request& reqHdr,
                                PrepForMigrationRpc::Response& respHdr,
                                Rpc& rpc)
{
    // Decide if we want to decline this request.

    // Ensure that there's no tablet overlap, just in case.
    bool overlap = (getTableForHash(reqHdr.tableId, reqHdr.firstKey) != NULL ||
                    getTableForHash(reqHdr.tableId, reqHdr.lastKey) != NULL);
    if (overlap) {
        LOG(WARNING, "already have tablet in range [%lu, %lu] for tableId %lu",
            reqHdr.firstKey, reqHdr.lastKey, reqHdr.tableId);
        respHdr.common.status = STATUS_OBJECT_EXISTS;
        return;
    }

    // Add the tablet to our map and mark it as RECOVERING so that no requests
    // are served on it.
    ProtoBuf::Tablets_Tablet& tablet(*tablets.add_tablet());
    tablet.set_table_id(reqHdr.tableId);
    tablet.set_start_key_hash(reqHdr.firstKey);
    tablet.set_end_key_hash(reqHdr.lastKey);
    tablet.set_state(ProtoBuf::Tablets_Tablet_State_RECOVERING);

    Table* table = new Table(reqHdr.tableId, reqHdr.firstKey,
                             reqHdr.lastKey);
    tablet.set_user_data(reinterpret_cast<uint64_t>(table));

    // TODO(rumble) would be nice to have a method to get a SL from an Rpc
    // object.
    LOG(NOTICE, "Ready to receive tablet from \"??\". Table %lu, "
        "range [%lu,%lu]", reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);
}

/**
 * Top-level server method to handle the MIGRATE_TABLET request.
 *
 * This is used to manually initiate the migration of a tablet (or piece of a
 * tablet) that this master owns to another master.
 *
 * \copydetails Service::ping
 */
void
MasterService::migrateTablet(const MigrateTabletRpc::Request& reqHdr,
                             MigrateTabletRpc::Response& respHdr,
                             Rpc& rpc)
{
    uint64_t tableId = reqHdr.tableId;
    uint64_t firstKey = reqHdr.firstKey;
    uint64_t lastKey = reqHdr.lastKey;
    ServerId newOwnerMasterId(reqHdr.newOwnerMasterId);

    // Find the tablet we're trying to move. We only support migration
    // when the tablet to be migrated consists of a range within a single,
    // contiguous tablet of ours.
    const ProtoBuf::Tablets::Tablet* tablet = NULL;
    int tabletIndex = 0;
    foreach (const ProtoBuf::Tablets::Tablet& i, tablets.tablet()) {
        if (tableId == i.table_id() &&
          firstKey >= i.start_key_hash() &&
          lastKey <= i.end_key_hash()) {
            tablet = &i;
            break;
        }
        tabletIndex++;
    }

    if (tablet == NULL) {
        LOG(WARNING, "Migration request for range this master does not "
            "own. TableId %lu, range [%lu,%lu]", tableId, firstKey, lastKey);
        respHdr.common.status = STATUS_UNKNOWN_TABLE;
        return;
    }

    if (newOwnerMasterId == serverId) {
        LOG(WARNING, "Migrating to myself doesn't make much sense");
        respHdr.common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    }

    Transport::SessionRef session = serverList.getSession(newOwnerMasterId);
    MasterClient recipient(session);

    Table* table = reinterpret_cast<Table*>(tablet->user_data());

    // TODO(rumble/slaughter) what if we end up splitting?!?

    // TODO(rumble/slaughter) add method to query TabletProfiler for # objs,
    // # bytes in a range in order for this to really work, we'll need to
    // split on a bucket
    // boundary. Otherwise we can't tell where bytes are in the chosen range.
    recipient.prepForMigration(tableId, firstKey, lastKey, 0, 0);

    LOG(NOTICE, "Migrating tablet (id %lu, first %lu, last %lu) to "
        "ServerId %lu (\"%s\")", tableId, firstKey, lastKey,
        *newOwnerMasterId, session->getServiceLocator().c_str());

    // We'll send over objects in Segment containers for better network
    // efficiency and convenience.
    void* transferBuf = Memory::xmemalign(HERE, 8*1024*1024, 8*1024*1024);
    Tub<Segment> transferSeg;

    // TODO(rumble/slaughter): These should probably be metrics.
    uint64_t totalObjects = 0;
    uint64_t totalTombstones = 0;
    uint64_t totalBytes = 0;

    // Hold on the the iterator since it locks the head Segment, avoiding any
    // additional appends once we've finished iterating.
    LogIterator it(log);
    for (; !it.isDone(); it.next()) {
        LogEntryHandle h = it.getHandle();
        if (h->type() == LOG_ENTRY_TYPE_OBJ) {
            const Object* logObj = h->userData<Object>();

            // Skip if not applicable.
            if (logObj->tableId != tableId)
                continue;

            if (logObj->keyHash() < firstKey || logObj->keyHash() > lastKey)
                continue;

            // Only send objects when they're currently in the hash table (
            // otherwise they're dead).
            LogEntryHandle curHandle = objectMap.lookup(logObj->tableId,
                                                        logObj->getKey(),
                                                        logObj->keyLength);
            if (curHandle == NULL)
                continue;

            if (curHandle->type() != LOG_ENTRY_TYPE_OBJ)
                continue;

            // NB: The cleaner is currently locked out due to the global
            //     objectUpdateLock. In the future this may not be the
            //     case and objects may be moved forward during iteration.
            if (curHandle->userData<Object>() != logObj)
                continue;

            totalObjects++;
        } else if (h->type() == LOG_ENTRY_TYPE_OBJTOMB) {
            const ObjectTombstone* logTomb = h->userData<ObjectTombstone>();

            // Skip if not applicable.
            if (logTomb->tableId != tableId)
                continue;

            if (logTomb->keyHash() < firstKey || logTomb->keyHash() > lastKey)
                continue;

            // We must always send tombstones, since an object we may have sent
            // could have been deleted more recently. We could be smarter and
            // more selective here, but that'd require keeping extra state to
            // know what we've already sent.
            //
            // TODO(rumble/slaughter) Actually, we can do better. The stupid way
            //      is to track each object or tombstone we've sent. The smarter
            //      way is to just record the LogPosition when we started
            //      iterating and only send newer tombstones.

            totalTombstones++;
        } else {
            // We're not interested in any other types.
            continue;
        }

        totalBytes += h->totalLength();

        if (!transferSeg)
            transferSeg.construct(-1, -1, transferBuf, 8*1024*1024);

        // If we can't fit it, send the current buffer and retry.
        if (transferSeg->append(it.getHandle(), false) == NULL) {
            transferSeg->close(NULL, false);
            recipient.receiveMigrationData(tableId,
                                          firstKey,
                                          transferSeg->getBaseAddress(),
                                          transferSeg->getTotalBytesAppended());
            LOG(DEBUG, "Sending migration segment");

            transferSeg.destroy();
            transferSeg.construct(-1, -1, transferBuf, 8*1024*1024);

            // If it doesn't fit this time, we're in trouble.
            if (transferSeg->append(it.getHandle(), false) == NULL) {
                LOG(ERROR, "Tablet migration failed: could not fit object "
                    "into empty segment (obj bytes %u)",
                    it.getHandle()->length());
                respHdr.common.status = STATUS_INTERNAL_ERROR;
                transferSeg.destroy();
                free(transferBuf);
                return;
            }
        }
    }

    if (transferSeg) {
        transferSeg->close(NULL, false);
        recipient.receiveMigrationData(tableId,
                                        firstKey,
                                        transferSeg->getBaseAddress(),
                                        transferSeg->getTotalBytesAppended());
        LOG(DEBUG, "Sending last migration segment");
        transferSeg.destroy();
    }

    free(transferBuf);

    // Now that all data has been transferred, we can reassign ownership of
    // the tablet. If this succeeds, we are free to drop the tablet. The
    // data is all on the other machine and the coordinator knows to use it
    // for any recoveries.
    coordinator->reassignTabletOwnership(
        tableId, firstKey, lastKey, newOwnerMasterId);

    LOG(NOTICE, "Tablet migration succeeded. Sent %lu objects and %lu "
        "tombstones. %lu bytes in total.", totalObjects, totalTombstones,
        totalBytes);

    tablets.mutable_tablet()->SwapElements(tablets.tablet_size() - 1,
                                           tabletIndex);
    tablets.mutable_tablet()->RemoveLast();
    free(table);
}

/**
 * Top-level server method to handle the RECEIVE_MIGRATION_DATA request.
 *
 * This RPC delivers tablet data to be added to a master during migration.
 * It must have been preceeded by an appropriate PREP_FOR_MIGRATION rpc.
 *
 * \copydetails Service::ping
 */
void
MasterService::receiveMigrationData(
    const ReceiveMigrationDataRpc::Request& reqHdr,
    ReceiveMigrationDataRpc::Response& respHdr,
    Rpc& rpc)
{
    uint64_t tableId = reqHdr.tableId;
    uint64_t firstKey = reqHdr.firstKey;
    uint32_t segmentBytes = reqHdr.segmentBytes;

    // TODO(rumble/slaughter) need to make sure we already have a table
    // created that was previously prepped for migration.
    const ProtoBuf::Tablets::Tablet* tablet = NULL;
    foreach (const ProtoBuf::Tablets::Tablet& i, tablets.tablet()) {
        if (tableId == i.table_id() && firstKey == i.start_key_hash()) {
            tablet = &i;
            break;
        }
    }

    if (tablet == NULL) {
        LOG(WARNING, "migration data received for unknown tablet %lu, "
            "firstKey %lu", tableId, firstKey);
        respHdr.common.status = STATUS_UNKNOWN_TABLE;
        return;
    }

    if (tablet->state() != ProtoBuf::Tablets_Tablet_State_RECOVERING) {
        LOG(WARNING, "migration data received for tablet not in the "
            "RECOVERING state (state = %s)!",
            ProtoBuf::Tablets_Tablet_State_Name(tablet->state()).c_str());
        // TODO(rumble/slaughter): better error code here?
        respHdr.common.status = STATUS_INTERNAL_ERROR;
        return;
    }

    LOG(NOTICE, "RECEIVED MIGRATION DATA (tbl %lu, fk %lu, bytes %u)!\n",
        tableId, firstKey, segmentBytes);

    rpc.requestPayload.truncateFront(sizeof(reqHdr));
    if (rpc.requestPayload.getTotalLength() != segmentBytes) {
        LOG(ERROR, "RPC size (%u) does not match advertised length (%u)",
            rpc.requestPayload.getTotalLength(),
            segmentBytes);
        respHdr.common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    }
    const void* segmentMemory = rpc.requestPayload.getStart<const void*>();
    recoverSegment(-1, segmentMemory, segmentBytes);

    // TODO(rumble/slaughter) what about tablet version numbers?
    //          - need to be made per-server now, no? then take max of two?
    //            but this needs to happen at the end (after head on orig.
    //            master is locked)
    //    - what about autoincremented keys?
    //    - what if we didn't send a whole tablet, but rather split one?!
    //      how does this affect autoincr. keys and the version number(s),
    //      if at all?
}

/**
 * Callback used to purge the tombstones from the hash table. Invoked by
 * HashTable::forEach.
 */
void
recoveryCleanup(LogEntryHandle maybeTomb, void *cookie)
{
    if (maybeTomb->type() == LOG_ENTRY_TYPE_OBJTOMB) {
        const ObjectTombstone *tomb =
                            maybeTomb->userData<ObjectTombstone>();
        MasterService *server = reinterpret_cast<MasterService*>(cookie);
        bool r = server->objectMap.remove(tomb->tableId,
                                          tomb->getKey(),
                                          tomb->keyLength);
        assert(r);
        // Tombstones are not explicitly freed in the log. The cleaner will
        // figure out that they're dead.
    }
}

/**
 * A Dispatch::Poller which lazily removes tombstones from the main HashTable.
 */
class RemoveTombstonePoller : public Dispatch::Poller {
  public:
    /**
     * Clean tombstones from #objectMap lazily and in the background.
     *
     * Instances of this class must be allocated with new since they
     * delete themselves when the #objectMap scan is completed which
     * automatically deregisters it from Dispatch.
     *
     * \param masterService
     *      The instance of MasterService which owns the #objectMap.
     * \param objectMap
     *      The HashTable which will be purged of tombstones.
     */
    RemoveTombstonePoller(MasterService& masterService,
                          HashTable<LogEntryHandle>& objectMap)
        : Dispatch::Poller(*Context::get().dispatch)
        , currentBucket(0)
        , masterService(masterService)
        , objectMap(objectMap)
    {
        LOG(NOTICE, "Starting cleanup of tombstones in background");
    }

    /**
     * Remove tombstones from a single bucket and yield to other work
     * in the system.
     */
    virtual void
    poll()
    {
        // This method runs in the dispatch thread, so it isn't safe to
        // manipulate any of the objectMap state if any RPCs are currently
        // executing.
        if (!Context::get().serviceManager->idle())
            return;
        objectMap.forEachInBucket(
            recoveryCleanup, &masterService, currentBucket);
        ++currentBucket;
        if (currentBucket == objectMap.getNumBuckets()) {
            LOG(NOTICE, "Cleanup of tombstones complete");
            delete this;
        }
    }

  private:
    /// Which bucket of #objectMap should be cleaned out next.
    uint64_t currentBucket;

    /// The MasterService used by the #recoveryCleanup callback.
    MasterService& masterService;

    /// The hash table to be purged of tombstones.
    HashTable<LogEntryHandle>& objectMap;

    DISALLOW_COPY_AND_ASSIGN(RemoveTombstonePoller);
};

/**
 * Remove leftover tombstones in the hash table added during recovery.
 * This method exists independently for testing purposes.
 */
void
MasterService::removeTombstones()
{
    CycleCounter<RawMetric> _(&metrics->master.removeTombstoneTicks);
#if TESTING
    // Asynchronous tombstone removal raises hell in unit tests.
    objectMap.forEach(recoveryCleanup, this);
#else
    Dispatch::Lock lock;
    new RemoveTombstonePoller(*this, objectMap);
#endif
}

namespace MasterServiceInternal {
/**
 * Each object of this class is responsible for fetching recovery data
 * for a single segment from a single backup.
 */
class RecoveryTask {
  PUBLIC:
    RecoveryTask(ServerList& serverList,
                 ServerId masterId,
                 uint64_t partitionId,
                 MasterService::Replica& replica)
        : serverList(serverList)
        , masterId(masterId)
        , partitionId(partitionId)
        , replica(replica)
        , response()
        , client(serverList.getSession(replica.backupId))
        , startTime(Cycles::rdtsc())
        , rpc()
        , resendTime(0)
    {
        rpc.construct(client, masterId, replica.segmentId,
                      partitionId, response);
    }
    ~RecoveryTask()
    {
        if (rpc && !rpc->isReady()) {
            LOG(WARNING, "Task destroyed while RPC active: segment %lu, "
                    "server %s", replica.segmentId,
                    serverList.toString(replica.backupId).c_str());
        }
    }
    void resend() {
        LOG(DEBUG, "Resend %lu", replica.segmentId);
        response.reset();
        rpc.construct(client, masterId, replica.segmentId,
                      partitionId, response);
    }
    ServerList& serverList;
    ServerId masterId;
    uint64_t partitionId;
    MasterService::Replica& replica;
    Buffer response;
    BackupClient client;
    const uint64_t startTime;
    Tub<BackupClient::GetRecoveryData> rpc;
    /// If we have to retry a request, this variable indicates the rdtsc time at
    /// which we should retry.  0 means we're not waiting for a retry.
    uint64_t resendTime;
    DISALLOW_COPY_AND_ASSIGN(RecoveryTask);
};
} // namespace MasterServiceInternal
using namespace MasterServiceInternal; // NOLINT


/**
 * Increments the access statistics for each read and write operation
 * on the repsective tablet.
 * \param *table
 *      Pointer to the table object that is assosiated with each tablet.
 */
void
MasterService::incrementReadAndWriteStatistics(Table* table)
{
    table->statEntry.set_number_read_and_writes(
                                table->statEntry.number_read_and_writes() + 1);
}

/**
 * Look through \a backups and ensure that for each segment id that appears
 * in the list that at least one copy of that segment was replayed.
 *
 * \param masterId
 *      The id of the crashed master this recovery master is recovering for.
 *      Only used for logging detailed log information on failure.
 * \param partitionId
 *      The id of the partition of the crashed master this recovery master is
 *      recovering. Only used for logging detailed log information on failure.
 * \param replicas
 *      The list of replicas and their statuses to be checked to ensure
 *      recovery of this partition was successful.
 * \throw SegmentRecoveryFailedException
 *      If some segment was not recovered and the recovery master is not
 *      a valid replacement for the crashed master.
 */
void
MasterService::detectSegmentRecoveryFailure(
            const ServerId masterId,
            const uint64_t partitionId,
            const vector<MasterService::Replica>& replicas)
{
    std::unordered_set<uint64_t> failures;
    foreach (const auto& replica, replicas) {
        switch (replica.state) {
        case MasterService::Replica::State::OK:
            failures.erase(replica.segmentId);
            break;
        case MasterService::Replica::State::FAILED:
            failures.insert(replica.segmentId);
            break;
        case MasterService::Replica::State::WAITING:
        case MasterService::Replica::State::NOT_STARTED:
        default:
            assert(false);
            break;
        }
    }
    if (!failures.empty()) {
        LOG(ERROR, "Recovery master failed to recover master %lu "
            "partition %lu", *masterId, partitionId);
        foreach (auto segmentId, failures)
            LOG(ERROR, "Unable to recover segment %lu", segmentId);
        throw SegmentRecoveryFailedException(HERE);
    }
}

/**
 * Helper for public recover() method.
 * Collect all the filtered log segments from backups for a set of tablets
 * formerly belonging to a crashed master which is being recovered and pass
 * them to the recovery master to have them replayed.
 *
 * \param masterId
 *      The id of the crashed master for which recoveryMaster will be taking
 *      over ownership of tablets.
 * \param partitionId
 *      The partition id of tablets inside the crashed master's will that
 *      this master is recovering.
 * \param replicas
 *      A list specifying for each segmentId a backup who can provide a
 *      filtered recovery data segment. A particular segment may be listed more
 *      than once if it has multiple viable backups.
 * \throw SegmentRecoveryFailedException
 *      If some segment was not recovered and the recovery master is not
 *      a valid replacement for the crashed master.
 */
void
MasterService::recover(ServerId masterId,
                       uint64_t partitionId,
                       vector<Replica>& replicas)
{
    /* Overview of the internals of this method and its structures.
     *
     * The main data structure is "replicas".  It works like a
     * scoreboard, tracking which segments have requests to backup
     * servers in-flight for data, which have been replayed, and
     * which have failed and must be replayed by another entry in
     * the table.
     *
     * replicasEnd is an iterator to the end of the segment replica list
     * which aids in tracking when the function is out of work.
     *
     * notStarted tracks the furtherest entry into the list which
     * has not been requested from a backup yet (State::NOT_STARTED).
     *
     * Here is a sample of what the structure might look like
     * during execution:
     *
     * backupId     segmentId  state
     * --------     ---------  -----
     *   8            99       OK
     *   3            88       FAILED
     *   1            77       OK
     *   2            77       OK
     *   6            88       WAITING
     *   2            66       NOT_STARTED  <- notStarted
     *   3            55       WAITING
     *   1            66       NOT_STARTED
     *   7            66       NOT_STARTED
     *   3            99       OK
     *
     * The basic idea is, the code kicks off up to some fixed
     * number worth of RPCs marking them WAITING starting from the
     * top of the list working down.  When a response comes it
     * marks the entry as FAILED if there was an error fetching or
     * replaying it. If it succeeded in replaying, though then ALL
     * entries for that segment_id are marked OK. (This is done
     * by marking the entry itself and then iterating starting
     * at "notStarted" and checking each row for a match).
     *
     * One other structure "runningSet" tracks which segment_ids
     * have RPCs in-flight.  When starting new RPCs rows that
     * have a segment_id that is in the set are skipped over.
     * However, since the row is still NOT_STARTED, notStarted
     * must point to it or to an earlier entry, so the entry
     * will be revisited in the case the other in-flight request
     * fails.  If the other request succeeds then the previously
     * skipped entry is marked OK and notStarted is advanced (if
     * possible).
     */
    uint64_t usefulTime = 0;
    uint64_t start = Cycles::rdtsc();
    LOG(NOTICE, "Recovering master %lu, partition %lu, %lu replicas available",
        *masterId, partitionId, replicas.size());

    std::unordered_set<uint64_t> runningSet;
    Tub<RecoveryTask> tasks[4];
    uint32_t activeRequests = 0;

    auto notStarted = replicas.begin();
    auto replicasEnd = replicas.end();

    // Start RPCs
    auto replicaIt = notStarted;
    foreach (auto& task, tasks) {
        while (!task) {
            if (replicaIt == replicasEnd)
                goto doneStartingInitialTasks;
            auto& replica = *replicaIt;
            LOG(DEBUG, "Starting getRecoveryData from %s for segment %lu "
                "on channel %ld (initial round of RPCs)",
                serverList.toString(replica.backupId).c_str(),
                replica.segmentId,
                &task - &tasks[0]);
            try {
                task.construct(serverList, masterId, partitionId, replica);
                replica.state = Replica::State::WAITING;
                runningSet.insert(replica.segmentId);
                ++metrics->master.segmentReadCount;
                ++activeRequests;
            } catch (const TransportException& e) {
                LOG(WARNING, "Couldn't contact %s, trying next backup; "
                    "failure was: %s",
                    serverList.toString(replica.backupId).c_str(),
                    e.str().c_str());
                replica.state = Replica::State::FAILED;
            } catch (const ServerListException& e) {
                LOG(WARNING, "No record of backup ID %lu, trying next backup",
                    replica.backupId.getId());
                replica.state = Replica::State::FAILED;
            }
            ++replicaIt;
            while (replicaIt != replicasEnd &&
                   contains(runningSet, replicaIt->segmentId)) {
                ++replicaIt;
            }
        }
    }
  doneStartingInitialTasks:

    // As RPCs complete, process them and start more
    Tub<CycleCounter<RawMetric>> readStallTicks;

    bool gotFirstGRD = false;

    std::unordered_multimap<uint64_t, Replica*> segmentIdToBackups;
    foreach (Replica& replica, replicas)
        segmentIdToBackups.insert({replica.segmentId, &replica});

    while (activeRequests) {
        if (!readStallTicks)
            readStallTicks.construct(&metrics->master.segmentReadStallTicks);
        replicaManager.proceed();
        uint64_t currentTime = Cycles::rdtsc();
        foreach (auto& task, tasks) {
            if (!task)
                continue;
            if (task->resendTime != 0) {
                if (currentTime > task->resendTime) {
                    task->resendTime = 0;
                    task->resend();
                }
                continue;
            }
            if (!task->rpc->isReady())
                continue;
            readStallTicks.destroy();
            LOG(DEBUG, "Waiting on recovery data for segment %lu from %s",
                task->replica.segmentId,
                serverList.toString(task->replica.backupId).c_str());
            try {
                (*task->rpc)();
                uint64_t grdTime = Cycles::rdtsc() - task->startTime;
                metrics->master.segmentReadTicks += grdTime;

                if (!gotFirstGRD) {
                    metrics->master.replicationBytes =
                        0 - metrics->transport.transmit.byteCount;
                    gotFirstGRD = true;
                }
                LOG(DEBUG, "Got getRecoveryData response from %s, took %.1f us "
                    "on channel %ld",
                    serverList.toString(task->replica.backupId).c_str(),
                    Cycles::toSeconds(grdTime)*1e06,
                    &task - &tasks[0]);

                uint32_t responseLen = task->response.getTotalLength();
                metrics->master.segmentReadByteCount += responseLen;
                LOG(DEBUG, "Recovering segment %lu with size %u",
                    task->replica.segmentId, responseLen);
                uint64_t startUseful = Cycles::rdtsc();
                recoverSegment(task->replica.segmentId,
                               task->response.getRange(0, responseLen),
                               responseLen);
                usefulTime += Cycles::rdtsc() - startUseful;

                runningSet.erase(task->replica.segmentId);
                // Mark this and any other entries for this segment as OK.
                LOG(DEBUG, "Checking %s off the list for %lu",
                    serverList.toString(task->replica.backupId).c_str(),
                    task->replica.segmentId);
                task->replica.state = Replica::State::OK;
                foreach (auto it, segmentIdToBackups.equal_range(
                                        task->replica.segmentId)) {
                    Replica& otherReplica = *it.second;
                    LOG(DEBUG, "Checking %s off the list for %lu",
                        serverList.toString(otherReplica.backupId).c_str(),
                        otherReplica.segmentId);
                    otherReplica.state = Replica::State::OK;
                }
            } catch (const RetryException& e) {
                // The backup isn't ready yet, try back in 1 ms.
                task->resendTime = currentTime +
                    static_cast<int>(Cycles::perSecond()/1000.0);
                continue;
            } catch (const TransportException& e) {
                LOG(WARNING, "Couldn't contact %s for segment %lu, "
                    "trying next backup; failure was: %s",
                    serverList.toString(task->replica.backupId).c_str(),
                    task->replica.segmentId,
                    e.str().c_str());
                task->replica.state = Replica::State::FAILED;
                runningSet.erase(task->replica.segmentId);
            } catch (const ClientException& e) {
                LOG(WARNING, "getRecoveryData failed on %s, "
                    "trying next backup; failure was: %s",
                    serverList.toString(task->replica.backupId).c_str(),
                    e.str().c_str());
                task->replica.state = Replica::State::FAILED;
                runningSet.erase(task->replica.segmentId);
            }

            task.destroy();

            // move notStarted up as far as possible
            while (notStarted != replicasEnd &&
                   notStarted->state != Replica::State::NOT_STARTED) {
                ++notStarted;
            }

            // Find the next NOT_STARTED entry that isn't in-flight
            // from another entry.
            auto replicaIt = notStarted;
            while (!task && replicaIt != replicasEnd) {
                while (replicaIt->state != Replica::State::NOT_STARTED ||
                       contains(runningSet, replicaIt->segmentId)) {
                    ++replicaIt;
                    if (replicaIt == replicasEnd)
                        goto outOfHosts;
                }
                Replica& replica = *replicaIt;
                LOG(DEBUG, "Starting getRecoveryData from %s for segment %lu "
                    "on channel %ld (after RPC completion)",
                    serverList.toString(replica.backupId).c_str(),
                    replica.segmentId,
                    &task - &tasks[0]);
                try {
                    task.construct(serverList, masterId, partitionId, replica);
                    replica.state = Replica::State::WAITING;
                    runningSet.insert(replica.segmentId);
                    ++metrics->master.segmentReadCount;
                } catch (const TransportException& e) {
                    LOG(WARNING, "Couldn't contact %s, trying next backup; "
                        "failure was: %s",
                        serverList.toString(replica.backupId).c_str(),
                        e.str().c_str());
                    replica.state = Replica::State::FAILED;
                } catch (const ServerListException& e) {
                    LOG(WARNING, "No record of backup ID %lu, "
                        "trying next backup",
                        replica.backupId.getId());
                    replica.state = Replica::State::FAILED;
                }
            }
          outOfHosts:
            if (!task)
                --activeRequests;
        }
    }
    readStallTicks.destroy();

    detectSegmentRecoveryFailure(masterId, partitionId, replicas);

    {
        CycleCounter<RawMetric> logSyncTicks(&metrics->master.logSyncTicks);
        LOG(NOTICE, "Syncing the log");
        metrics->master.logSyncBytes =
            0 - metrics->transport.transmit.byteCount;
        log.sync();
        metrics->master.logSyncBytes += metrics->transport.transmit.byteCount;
    }

    metrics->master.replicationBytes += metrics->transport.transmit.byteCount;

    double totalSecs = Cycles::toSeconds(Cycles::rdtsc() - start);
    double usefulSecs = Cycles::toSeconds(usefulTime);
    LOG(NOTICE, "Recovery complete, took %.1f ms, useful replaying "
        "time %.1f ms (%.1f%% effective)",
        totalSecs * 1e03,
        usefulSecs * 1e03,
        100 * usefulSecs / totalSecs);
}

/**
 * Top-level server method to handle the RECOVER request.
 * \copydetails Service::ping
 */
void
MasterService::recover(const RecoverRpc::Request& reqHdr,
                       RecoverRpc::Response& respHdr,
                       Rpc& rpc)
{
    CycleCounter<RawMetric> recoveryTicks(&metrics->master.recoveryTicks);
    metrics->master.recoveryCount++;
    metrics->master.replicas = replicaManager.numReplicas;

    ServerId masterId(reqHdr.masterId);
    uint64_t partitionId = reqHdr.partitionId;
    ProtoBuf::Tablets recoveryTablets;
    ProtoBuf::parseFromResponse(rpc.requestPayload, sizeof(reqHdr),
                                reqHdr.tabletsLength, recoveryTablets);

    uint32_t offset = (downCast<uint32_t>(sizeof(reqHdr)) +
                       reqHdr.tabletsLength);
    vector<Replica> replicas;
    replicas.reserve(reqHdr.numReplicas);
    for (uint32_t i = 0; i < reqHdr.numReplicas; ++i) {
        const RecoverRpc::Replica* replicaLocation =
            rpc.requestPayload.getOffset<RecoverRpc::Replica>(offset);
        offset += downCast<uint32_t>(sizeof(RecoverRpc::Replica));
        Replica replica(replicaLocation->backupId,
                        replicaLocation->segmentId);
        replicas.push_back(replica);
    }
    LOG(DEBUG, "Starting recovery of %u tablets on masterId %lu",
        recoveryTablets.tablet_size(), serverId.getId());
    rpc.sendReply();

    // reqHdr, respHdr, and rpc are off-limits now

    // Install tablets we are recovering and mark them as such (we don't
    // own them yet).
    vector<ProtoBuf::Tablets_Tablet*> newTablets;
    foreach (const ProtoBuf::Tablets::Tablet& tablet,
             recoveryTablets.tablet()) {
        ProtoBuf::Tablets_Tablet& newTablet(*tablets.add_tablet());
        newTablet = tablet;
        Table* table = new Table(newTablet.table_id(),
                                 newTablet.start_key_hash(),
                                 newTablet.end_key_hash());
        newTablet.set_user_data(reinterpret_cast<uint64_t>(table));
        newTablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        newTablets.push_back(&newTablet);
    }

    // Record the log position before recovery started.
    LogPosition headOfLog = log.headOfLog();

    // Recover Segments, firing MasterService::recoverSegment for each one.
    recover(masterId, partitionId, replicas);

    // Free recovery tombstones left in the hash table.
    removeTombstones();

    // Once the coordinator and the recovery master agree that the
    // master has taken over for the tablets it can update its tables
    // and begin serving requests.

    // Update the recoveryTablets to reflect the fact that this master is
    // going to try to become the owner. The coordinator will assign final
    // ownership in response to the TABLETS_RECOVERED rpc (i.e. we'll only
    // be owners if the call succeeds. It could fail if the coordinator
    // decided to recover these tablets elsewhere instead).
    foreach (ProtoBuf::Tablets::Tablet& tablet,
             *recoveryTablets.mutable_tablet()) {
        LOG(NOTICE, "set tablet %lu %lu %lu to locator %s, id %lu",
                 tablet.table_id(), tablet.start_key_hash(),
                 tablet.end_key_hash(), config.localLocator.c_str(),
                 serverId.getId());
        tablet.set_service_locator(config.localLocator);
        tablet.set_server_id(serverId.getId());
        tablet.set_ctime_log_head_id(headOfLog.segmentId());
        tablet.set_ctime_log_head_offset(headOfLog.segmentOffset());
    }
    coordinator->tabletsRecovered(serverId, recoveryTablets);

    // TODO(anyone) Should delete tablets if tabletsRecovered returns
    //              failure. Also, should handle tabletsRecovered
    //              timing out.

    // Ok - we're expected to be serving now. Mark recovered tablets
    // as normal so we can handle clients.
    foreach (ProtoBuf::Tablets_Tablet* newTablet, newTablets)
        newTablet->set_state(ProtoBuf::Tablets::Tablet::NORMAL);
}

/**
 * Given a RecoverySegmentIterator for the Segment we're currently
 * recovering, advance it and issue prefetches on the hash tables.
 * This is used exclusively by recoverSegment().
 *
 * \param[in] i
 *      A RecoverySegmentIterator to use for prefetching. Note that this
 *      method modifies the iterator, so the caller should not use
 *      it for its own iteration.
 */
void
MasterService::recoverSegmentPrefetcher(RecoverySegmentIterator& i)
{
    i.next();

    if (i.isDone())
        return;

    LogEntryType type = i.getType();

    uint64_t tblId = ~0UL;
    const char* key = "";
    uint16_t keyLength = 0;

    if (type == LOG_ENTRY_TYPE_OBJ) {
        const Object *recoverObj =
            reinterpret_cast<const Object *>(i.getPointer());
        tblId = recoverObj->tableId;
        key = recoverObj->getKey();
        keyLength = recoverObj->keyLength;
    } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
        const ObjectTombstone *recoverTomb =
            reinterpret_cast<const ObjectTombstone *>(i.getPointer());
        tblId = recoverTomb->tableId;
        key = recoverTomb->getKey();
        keyLength = recoverTomb->keyLength;
    }

    objectMap.prefetchBucket(tblId, key, keyLength);
}

/**
 * Replay a filtered segment from a crashed Master that this Master is taking
 * over for.
 *
 * \param segmentId
 *      The segmentId of the segment as it was in the log of the crashed Master.
 * \param buffer
 *      A pointer to a valid segment which has been pre-filtered of all
 *      objects except those that pertain to the tablet ranges this Master
 *      will be responsible for after the recovery completes.
 * \param bufferLength
 *      Length of the buffer in bytes.
 */
void
MasterService::recoverSegment(uint64_t segmentId, const void *buffer,
                              uint32_t bufferLength)
{
    uint64_t startReplicationTicks = metrics->master.replicaManagerTicks;
    LOG(DEBUG, "recoverSegment %lu, ...", segmentId);
    CycleCounter<RawMetric> _(&metrics->master.recoverSegmentTicks);

    RecoverySegmentIterator i(buffer, bufferLength);
    RecoverySegmentIterator prefetch(buffer, bufferLength);

    uint64_t lastOffsetBackupProgress = 0;
    while (!i.isDone()) {
        LogEntryType type = i.getType();

        if (i.getOffset() > lastOffsetBackupProgress + 50000) {
            lastOffsetBackupProgress = i.getOffset();
            replicaManager.proceed();
        }

        recoverSegmentPrefetcher(prefetch);

        metrics->master.recoverySegmentEntryCount++;
        metrics->master.recoverySegmentEntryBytes += i.getLength();

        if (type == LOG_ENTRY_TYPE_OBJ) {
            const Object *recoverObj =
                  reinterpret_cast<const Object *>(i.getPointer());
            uint64_t tblId = recoverObj->tableId;
            const char* key = recoverObj->getKey();
            uint16_t keyLength = recoverObj->keyLength;

            const Object *localObj = NULL;
            const ObjectTombstone *tomb = NULL;
            LogEntryHandle handle = objectMap.lookup(tblId, key, keyLength);
            if (handle != NULL) {
                if (handle->type() == LOG_ENTRY_TYPE_OBJTOMB)
                    tomb = handle->userData<ObjectTombstone>();
                else
                    localObj = handle->userData<Object>();
            }

            // can't have both a tombstone and an object in the hash tables
            assert(tomb == NULL || localObj == NULL);

            uint64_t minSuccessor = 0;
            if (localObj != NULL)
                minSuccessor = localObj->version + 1;
            else if (tomb != NULL)
                minSuccessor = tomb->objectVersion + 1;

            if (recoverObj->version >= minSuccessor) {
                // write to log (with lazy backup flush) & update hash table
                LogEntryHandle newObjHandle = log.append(LOG_ENTRY_TYPE_OBJ,
                    recoverObj, i.getLength(), false, i.checksum());
                ++metrics->master.objectAppendCount;
                metrics->master.liveObjectBytes +=
                    recoverObj->dataLength(i.getLength());

                // The TabletProfiler is updated asynchronously.
                objectMap.replace(newObjHandle);

                // The cleaner will figure out that the tombstone is dead.

                // nuke the old object, if it existed
                if (localObj != NULL) {
                    metrics->master.liveObjectBytes -=
                        localObj->dataLength(handle->length());
                    log.free(handle);
                } else {
                    ++metrics->master.liveObjectCount;
                }
            } else {
                ++metrics->master.objectDiscardCount;
            }
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            const ObjectTombstone *recoverTomb =
                  reinterpret_cast<const ObjectTombstone *>(i.getPointer());
            uint64_t tblId = recoverTomb->tableId;
            const char* key = recoverTomb->getKey();
            uint16_t keyLength = recoverTomb->keyLength;

            bool checksumIsValid = ({
                CycleCounter<RawMetric> c(&metrics->master.verifyChecksumTicks);
                i.isChecksumValid();
            });
            if (!checksumIsValid) {
                LOG(WARNING, "invalid tombstone checksum! tbl: %lu, obj: %.*s, "
                    "ver: %lu", tblId, keyLength, key,
                    recoverTomb->objectVersion);
            }

            const Object *localObj = NULL;
            const ObjectTombstone *tomb = NULL;
            LogEntryHandle handle = objectMap.lookup(tblId, key, keyLength);
            if (handle != NULL) {
                if (handle->type() == LOG_ENTRY_TYPE_OBJTOMB)
                    tomb = handle->userData<ObjectTombstone>();
                else
                    localObj = handle->userData<Object>();
            }

            // can't have both a tombstone and an object in the hash tables
            assert(tomb == NULL || localObj == NULL);

            uint64_t minSuccessor = 0;
            if (localObj != NULL)
                minSuccessor = localObj->version;
            else if (tomb != NULL)
                minSuccessor = tomb->objectVersion + 1;

            if (recoverTomb->objectVersion >= minSuccessor) {
                ++metrics->master.tombstoneAppendCount;
                LogEntryHandle newTomb =
                    log.append(LOG_ENTRY_TYPE_OBJTOMB, recoverTomb,
                               recoverTomb->tombLength(), false, i.checksum());
                objectMap.replace(newTomb);

                // The cleaner will figure out that the tombstone is dead.

                // nuke the object, if it existed
                if (localObj != NULL) {
                    --metrics->master.liveObjectCount;
                    metrics->master.liveObjectBytes -=
                        localObj->dataLength(handle->length());
                    log.free(handle);
                }
            } else {
                ++metrics->master.tombstoneDiscardCount;
            }
        }

        i.next();
    }
    LOG(DEBUG, "Segment %lu replay complete", segmentId);
    metrics->master.backupInRecoverTicks +=
        metrics->master.replicaManagerTicks - startReplicationTicks;
}

/**
 * Top-level server method to handle the REMOVE request.
 *
 * \copydetails MasterService::read
 */
void
MasterService::remove(const RemoveRpc::Request& reqHdr,
                      RemoveRpc::Response& respHdr,
                      Rpc& rpc)
{
    const char* key = static_cast<const char*>(rpc.requestPayload.getRange(
                      downCast<uint32_t>(sizeof(reqHdr)), reqHdr.keyLength));

    Table* table = getTable(reqHdr.tableId, key, reqHdr.keyLength);
    if (table == NULL) {
        respHdr.common.status = STATUS_UNKNOWN_TABLE;
        return;
    }

    LogEntryHandle handle = objectMap.lookup(reqHdr.tableId,
                                             key, reqHdr.keyLength);
    if (handle == NULL || handle->type() != LOG_ENTRY_TYPE_OBJ) {
        Status status = rejectOperation(reqHdr.rejectRules,
                                        VERSION_NONEXISTENT);
        if (status != STATUS_OK)
            respHdr.common.status = status;
        return;
    }

    const Object *obj = handle->userData<Object>();
    respHdr.version = obj->version;

    // Abort if we're trying to delete the wrong version.
    Status status = rejectOperation(reqHdr.rejectRules, respHdr.version);
    if (status != STATUS_OK) {
        respHdr.common.status = status;
        return;
    }

    DECLARE_OBJECTTOMBSTONE(tomb, obj->keyLength,
                                  log.getSegmentId(obj), obj);

    // Write the tombstone into the Log, increment the tablet version
    // number, and remove from the hash table.
    try {
        log.append(LOG_ENTRY_TYPE_OBJTOMB, &tomb, tomb->tombLength());
    } catch (LogException& e) {
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        respHdr.common.status = STATUS_RETRY;
        return;
    }

    table->RaiseVersion(obj->version + 1);
    log.free(handle);
    objectMap.remove(reqHdr.tableId, key, reqHdr.keyLength);
}

/**
 * Top-level server method to handle the INCREMENT request.
 *
 * \copydetails MasterService::read
 */
void
MasterService::increment(const IncrementRpc::Request& reqHdr,
                     IncrementRpc::Response& respHdr,
                     Rpc& rpc)
{
    //Read the current value of the object and add the increment value
    uint32_t reqOffset = downCast<uint32_t>(sizeof(reqHdr));
    const char* key = static_cast<const char*>(rpc.requestPayload.getRange(
                                               reqOffset, reqHdr.keyLength));

    if (getTable(reqHdr.tableId, key, reqHdr.keyLength) == NULL) {
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }

    LogEntryHandle handle = objectMap.lookup(reqHdr.tableId,
                                             key, reqHdr.keyLength);

    if (handle == NULL || handle->type() != LOG_ENTRY_TYPE_OBJ) {
        respHdr.common.status = STATUS_OBJECT_DOESNT_EXIST;
        return;
    }

    const Object* obj = handle->userData<Object>();
    Status status = rejectOperation(reqHdr.rejectRules, obj->version);
    if (status != STATUS_OK) {
        respHdr.common.status = status;
        return;
    }

    if (obj->dataLength(handle->length()) != 8) {
        respHdr.common.status = STATUS_INVALID_OBJECT;
        return;
    }

    int64_t oldValue;
    int64_t newValue;
    memcpy(&oldValue, obj->getData(), obj->dataLength(handle->length()));
    newValue = oldValue + reqHdr.incrementValue;

    //Write the new value back
    Buffer newValueBuffer;
    Buffer::Chunk::appendToBuffer(&newValueBuffer, rpc.requestPayload.getRange(
                                  reqOffset, reqHdr.keyLength),
                                  reqHdr.keyLength);
    Buffer::Chunk::appendToBuffer(&newValueBuffer, &newValue,
                                  sizeof(int64_t));

    status = storeData(reqHdr.tableId, &reqHdr.rejectRules,
                              &newValueBuffer,
                              static_cast<uint32_t>(0),
                              reqHdr.keyLength,
                              static_cast<uint32_t>(sizeof(int64_t)),
                              &respHdr.version, false);

    if (status != STATUS_OK) {
        respHdr.common.status = status;
        return;
    }

    //return new value
    respHdr.newValue = newValue;
}

/**
 * RPC handler for IS_REPLICA_NEEDED; indicates to backup servers whether
 * a replica for a particular segment that this master generated is needed
 * for durability or that it can be safely discarded.
 */
void
MasterService::isReplicaNeeded(const IsReplicaNeededRpc::Request& reqHdr,
                               IsReplicaNeededRpc::Response& respHdr,
                               Rpc& rpc)
{
    ServerId backupServerId = ServerId(reqHdr.backupServerId);
    respHdr.needed = replicaManager.isReplicaNeeded(backupServerId,
                                                    reqHdr.segmentId);
}

/**
 * Top-level server method to handle the WRITE request.
 *
 * \copydetails MasterService::read
 */
void
MasterService::write(const WriteRpc::Request& reqHdr,
                     WriteRpc::Response& respHdr,
                     Rpc& rpc)
{
    Status status = storeData(reqHdr.tableId, &reqHdr.rejectRules,
                              &rpc.requestPayload,
                              static_cast<uint32_t>(sizeof(reqHdr)),
                              reqHdr.keyLength,
                              static_cast<uint32_t>(reqHdr.length),
                              &respHdr.version, reqHdr.async);

    if (status != STATUS_OK) {
        respHdr.common.status = status;
        return;
    }
}

/**
 * Ensures that this master owns the tablet for the given object
 * based on its tableId and key and returns the corresponding Table.
 *
 * \param tableId
 *      Identifier for a desired table.
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 * \param keyLength
 *      Size in bytes of the key.
 *
 * \return
 *      The Table of which the tablet containing this object is a part,
 *      or NULL if this master does not own the tablet.
 */
Table*
MasterService::getTable(uint64_t tableId, const char* key, uint16_t keyLength)
{

    ProtoBuf::Tablets::Tablet const* tablet = getTabletForHash(tableId,
                                                        getKeyHash(key,
                                                                   keyLength));
    if (tablet == NULL)
        return NULL;

    Table* table = reinterpret_cast<Table*>(tablet->user_data());
    incrementReadAndWriteStatistics(table);
    return table;
}

/**
 * Ensures that this master owns the tablet for any object corresponding
 * to the given hash value of its string key and returns the
 * corresponding Table.
 *
 * \param tableId
 *      Identifier for a desired table.
 * \param keyHash
 *      Hash value of the variable length key of the object.
 *
 * \return
 *      The Table of which the tablet containing this object is a part,
 *      or NULL if this master does not own the tablet.
 */
Table*
MasterService::getTableForHash(uint64_t tableId, HashType keyHash)
{
    ProtoBuf::Tablets::Tablet const* tablet = getTabletForHash(tableId,
                                                               keyHash);
    if (tablet == NULL)
        return NULL;

    Table* table = reinterpret_cast<Table*>(tablet->user_data());
    return table;
}

/**
 * Ensures that this master owns the tablet for any object corresponding
 * to the given hash value of its string key and returns the
 * corresponding Table.
 *
 * \param tableId
 *      Identifier for a desired table.
 * \param keyHash
 *      Hash value of the variable length key of the object.
 *
 * \return
 *      The Table of which the tablet containing this object is a part,
 *      or NULL if this master does not own the tablet.
 */
ProtoBuf::Tablets::Tablet const*
MasterService::getTabletForHash(uint64_t tableId, HashType keyHash)
{
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet()) {
        if (tablet.table_id() == tableId &&
            tablet.start_key_hash() <= keyHash &&
            keyHash <= tablet.end_key_hash()) {
            return &tablet;
        }
    }
    return NULL;
}

/**
 * Check a set of RejectRules against the current state of an object
 * to decide whether an operation is allowed.
 *
 * \param rejectRules
 *      Specifies conditions under which the operation should fail.
 * \param version
 *      The current version of an object, or VERSION_NONEXISTENT
 *      if the object does not currently exist (used to test rejectRules)
 *
 * \return
 *      The return value is STATUS_OK if none of the reject rules
 *      indicate that the operation should be rejected. Otherwise
 *      the return value indicates the reason for the rejection.
 */
Status
MasterService::rejectOperation(const RejectRules& rejectRules, uint64_t version)
{
    if (version == VERSION_NONEXISTENT) {
        if (rejectRules.doesntExist)
            return STATUS_OBJECT_DOESNT_EXIST;
        return STATUS_OK;
    }
    if (rejectRules.exists)
        return STATUS_OBJECT_DOESNT_EXIST;
    if (rejectRules.versionLeGiven && version <= rejectRules.givenVersion)
        return STATUS_WRONG_VERSION;
    if (rejectRules.versionNeGiven && version != rejectRules.givenVersion)
        return STATUS_WRONG_VERSION;
    return STATUS_OK;
}

/**
 * Determine whether or not an object is still alive (i.e. is referenced
 * by the hash table). If so, the cleaner must perpetuate it. If not, it
 * can be safely discarded.
 *
 * \param[in] handle
 *      LogEntryHandle to the object whose liveness is being queried.
 * \param[in] cookie
 *      The opaque state pointer registered with the callback.
 * \return
 *      True if the object is still alive, else false.
 */
bool
objectLivenessCallback(LogEntryHandle handle, void* cookie)
{
    assert(handle->type() == LOG_ENTRY_TYPE_OBJ);

    MasterService* svr = static_cast<MasterService *>(cookie);
    assert(svr != NULL);

    const Object* evictObj = handle->userData<Object>();
    assert(evictObj != NULL);

    std::lock_guard<SpinLock> lock(svr->objectUpdateLock);

    Table* t = svr->getTable(evictObj->tableId,
                             evictObj->getKey(),
                             evictObj->keyLength);
    if (t == NULL)
        return false;

    LogEntryHandle hashTblHandle =
        svr->objectMap.lookup(evictObj->tableId,
                              evictObj->getKey(), evictObj->keyLength);
    if (hashTblHandle == NULL)
        return false;

    assert(hashTblHandle->type() == LOG_ENTRY_TYPE_OBJ);
    const Object *hashTblObj = hashTblHandle->userData<Object>();

    // simple pointer comparison suffices
    return (hashTblObj == evictObj);
}

/**
 * Callback used by the LogCleaner when it's cleaning a Segment and moves
 * an Object to a new Segment.
 *
 * The cleaner will have already invoked the liveness callback to see whether
 * or not the Object was recently live. Since it could no longer be live (it
 * may have been deleted or overwritten since the check), this callback must
 * decide if it is still live, atomically update any structures if needed, and
 * return whether or not any action has been taken so the caller will know
 * whether or not the new copy should be retained.
 *
 * \param[in] oldHandle
 *      LogEntryHandle to the object's old location that will soon be
 *      invalid.
 * \param[in] newHandle
 *      LogEntryHandle to the object's new location that already exists
 *      as a possible replacement, if needed.
 * \param[in] cookie
 *      The opaque state pointer registered with the callback.
 * \return
 *      True if newHandle is needed (i.e. it replaced oldHandle). False
 *      indicates that newHandle wasn't needed and can be immediately
 *      deleted.
 */
bool
objectRelocationCallback(LogEntryHandle oldHandle,
                         LogEntryHandle newHandle,
                         void* cookie)
{
    assert(oldHandle->type() == LOG_ENTRY_TYPE_OBJ);

    MasterService* svr = static_cast<MasterService *>(cookie);
    assert(svr != NULL);

    const Object* evictObj = oldHandle->userData<Object>();
    assert(evictObj != NULL);

    std::lock_guard<SpinLock> lock(svr->objectUpdateLock);

    Table* table = svr->getTable(evictObj->tableId,
                                 evictObj->getKey(),
                                 evictObj->keyLength);
    if (table == NULL) {
        // That tablet doesn't exist on this server anymore.
        // Just remove the hash table entry, if it exists.
        svr->objectMap.remove(evictObj->tableId,
                              evictObj->getKey(), evictObj->keyLength);
        return false;
    }

    LogEntryHandle hashTblHandle =
        svr->objectMap.lookup(evictObj->tableId,
                              evictObj->getKey(), evictObj->keyLength);

    bool keepNewObject = false;
    if (hashTblHandle != NULL) {
        assert(hashTblHandle->type() == LOG_ENTRY_TYPE_OBJ);
        const Object *hashTblObj = hashTblHandle->userData<Object>();

        // simple pointer comparison suffices
        keepNewObject = (hashTblObj == evictObj);
        if (keepNewObject) {
            svr->objectMap.replace(newHandle);
        }
    }

    // Update table statistics.
    if (!keepNewObject) {
        table->objectCount--;
        table->objectBytes -= oldHandle->length();
    }

    return keepNewObject;
}

/**
 * Callback used by the Log to determine the modification timestamp of an
 * Object. Timestamps are stored in the Object itself, rather than in the
 * Log, since not all Log entries need timestamps and other parts of the
 * system (or clients) may care about Object modification times.
 *
 * \param[in]  handle
 *      LogEntryHandle to the entry being examined.
 * \return
 *      The Object's modification timestamp.
 */
uint32_t
objectTimestampCallback(LogEntryHandle handle)
{
    assert(handle->type() == LOG_ENTRY_TYPE_OBJ);
    return handle->userData<Object>()->timestamp;
}

/**
 * Determine whether or not a tombstone is still alive (i.e. it references
 * a segment that still exists). If so, the cleaner must perpetuate it. If
 * not, it can be safely discarded.
 *
 * \param[in] handle
 *      LogEntryHandle to the object whose liveness is being queried.
 * \param[in] cookie
 *      The opaque state pointer registered with the callback.
 * \return
 *      True if the object is still alive, else false.
 */
bool
tombstoneLivenessCallback(LogEntryHandle handle, void* cookie)
{
    assert(handle->type() == LOG_ENTRY_TYPE_OBJTOMB);

    MasterService* svr = static_cast<MasterService *>(cookie);
    assert(svr != NULL);

    const ObjectTombstone* tomb = handle->userData<ObjectTombstone>();
    assert(tomb != NULL);

    return svr->log.isSegmentLive(tomb->segmentId);
}

/**
 * Callback used by the LogCleaner when it's cleaning a Segment and moves
 * a Tombstone to a new Segment.
 *
 * The cleaner will have already invoked the liveness callback to see whether
 * or not the Tombstone was recently live. Since it could no longer be live (it
 * may have been deleted or overwritten since the check), this callback must
 * decide if it is still live, atomically update any structures if needed, and
 * return whether or not any action has been taken so the caller will know
 * whether or not the new copy should be retained.
 *
 * \param[in] oldHandle
 *      LogEntryHandle to the Tombstones's old location that will soon be
 *      invalid.
 * \param[in] newHandle
 *      LogEntryHandle to the Tombstones's new location that already exists
 *      as a possible replacement, if needed.
 * \param[in] cookie
 *      The opaque state pointer registered with the callback.
 * \return
 *      True if newHandle is needed (i.e. it replaced oldHandle). False
 *      indicates that newHandle wasn't needed and can be immediately
 *      deleted.
 */
bool
tombstoneRelocationCallback(LogEntryHandle oldHandle,
                            LogEntryHandle newHandle,
                            void* cookie)
{
    assert(oldHandle->type() == LOG_ENTRY_TYPE_OBJTOMB);

    MasterService* svr = static_cast<MasterService *>(cookie);
    assert(svr != NULL);

    const ObjectTombstone* tomb = oldHandle->userData<ObjectTombstone>();
    assert(tomb != NULL);

    // see if the referent is still there
    bool keepNewTomb = svr->log.isSegmentLive(tomb->segmentId);

    Table* table = svr->getTable(tomb->tableId,
                                 tomb->getKey(),
                                 tomb->keyLength);
    if (table != NULL && !keepNewTomb) {
        table->tombstoneCount--;
        table->tombstoneBytes -= oldHandle->length();
    }

    return keepNewTomb;
}

/**
 * Callback used by the Log to determine the age of Tombstone. We don't
 * current store tombstone ages, so just return the current timstamp
 * (they're perpetually young). This needs to be re-thought.
 *
 * \param[in]  handle
 *      LogEntryHandle to the entry being examined.
 * \return
 *      The tombstone's creation timestamp.
 */
uint32_t
tombstoneTimestampCallback(LogEntryHandle handle)
{
    assert(handle->type() == LOG_ENTRY_TYPE_OBJTOMB);
    return handle->userData<ObjectTombstone>()->timestamp;
}

/**
 * \param tableId
 *      The table in which to store the object.
 * \param rejectRules
 *      Specifies conditions under which the write should be aborted with an
 *      error. May not be NULL.
 * \param keyAndData
 *      Contains the binary blob that includes the key and the
 *      data to store at (tableId, key).
 *      May not be NULL.
 * \param keyOffset
 *      The offset into \a keyAndData where the key begins.
 * \param keyLength
 *      The size in bytes of the key in the blob.
 * \param dataLength
 *      The size in bytes of the data in the blob. The data follows the key in
 *      the keyAndData blob.
 * \param newVersion
 *      The version number of the object is returned here. May not be NULL. If
 *      the operation was successful this will be the new version for the
 *      object; if this object has ever existed previously the new version is
 *      guaranteed to be greater than any previous version of the object. If
 *      the operation failed then the version number returned is the current
 *      version of the object, or VERSION_NONEXISTENT if the object does not
 *      exist.
 * \param async
 *      If true, the replication may happen sometime later.
 *      If false, this write will be replicated to backups before return.
 * \return
 *      STATUS_OK if the object was written. Otherwise, for example,
 *      STATUS_UKNOWN_TABLE may be returned.
 */
Status
MasterService::storeData(uint64_t tableId,
                         const RejectRules* rejectRules,
                         Buffer* keyAndData,
                         uint32_t keyOffset,
                         uint16_t keyLength,
                         uint32_t dataLength,
                         uint64_t* newVersion,
                         bool async)
{
    DECLARE_OBJECT(newObject, keyLength, dataLength);

    newObject->keyLength = keyLength;
    newObject->tableId = tableId;

    // Copy both the key and the data from keyAndData blob into keyAndData
    // field in newObject.
    // In this field, the data should immediately follow the key, like in
    // the keyAndData blob. Hence, we can copy both of them together.
    keyAndData->copy(keyOffset, keyLength + dataLength,
                     newObject->getKeyLocation());

    Table* table = getTable(tableId, newObject->getKey(), keyLength);
    if (table == NULL)
        return STATUS_UNKNOWN_TABLE;

    if (!anyWrites) {
        // This is the first write; use this as a trigger to update the
        // cluster configuration information and open a session with each
        // backup, so it won't slow down recovery benchmarks.  This is a
        // temporary hack, and needs to be replaced with a more robust
        // approach to updating cluster configuration information.
        anyWrites = true;

        // NULL coordinator means we're in test mode, so skip this.
        if (coordinator) {
            ProtoBuf::ServerList backups;
            coordinator->getBackupList(backups);
            TransportManager& transportManager =
                *Context::get().transportManager;
            foreach(auto& backup, backups.server())
                transportManager.getSession(backup.service_locator().c_str());
        }
    }

    const Object *obj = NULL;
    LogEntryHandle handle = objectMap.lookup(tableId,
                                             newObject->getKey(),
                                             keyLength);
    if (handle != NULL) {
        if (handle->type() == LOG_ENTRY_TYPE_OBJTOMB) {
            recoveryCleanup(handle,
                            this);
            handle = NULL;
        } else {
            assert(handle->type() == LOG_ENTRY_TYPE_OBJ);
            obj = handle->userData<Object>();
        }
    }

    uint64_t version = (obj != NULL) ? obj->version : VERSION_NONEXISTENT;

    Status status = rejectOperation(*rejectRules, version);
    if (status != STATUS_OK) {
        *newVersion = version;
        return status;
    }

    if (obj != NULL)
        newObject->version = obj->version + 1;
    else
        newObject->version = table->AllocateVersion();

    assert(obj == NULL || newObject->version > obj->version);

    // Perform a multi-append to atomically add the tombstone and
    // new object (if we need a tombstone for the prior one).
    LogMultiAppendVector appends;

    if (obj != NULL) {
        DECLARE_OBJECTTOMBSTONE(tomb, keyLength,
                                log.getSegmentId(obj), obj);
        appends.push_back({ LOG_ENTRY_TYPE_OBJTOMB,
                            tomb,
                            tomb->tombLength() });
    }

    try {
        appends.push_back({ LOG_ENTRY_TYPE_OBJ,
                            newObject,
                            newObject->objectLength(dataLength) });
        LogEntryHandleVector objHandles = log.multiAppend(appends, !async);
        if (obj == NULL) {
            objectMap.replace(objHandles[0]);
        } else {
            objectMap.replace(objHandles[1]);
            log.free(handle);
        }
        *newVersion = newObject->version;
        bytesWritten += keyLength + dataLength;
        return STATUS_OK;
    } catch (LogOutOfMemoryException& e) {
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        return STATUS_RETRY;
    }
}

} // namespace RAMCloud
