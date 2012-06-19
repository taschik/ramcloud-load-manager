/* Copyright (c) 2010-2011 Stanford University
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

#include "ObjectFinder.h"
#include "ShortMacros.h"
#include "KeyHash.h"
#include "KeyUtil.h"


#include <iostream>

namespace RAMCloud {

namespace {

/**
 * The implementation of ObjectFinder::TabletMapFetcher that is used for normal
 * execution. Simply forwards getTabletMap to the coordinator client.
 */
class RealTabletMapFetcher : public ObjectFinder::TabletMapFetcher {
  public:
    explicit RealTabletMapFetcher(CoordinatorClient& coordinator)
        : coordinator(coordinator)
    {
    }
    void getTabletMap(ProtoBuf::Tablets& tabletMap) {
        coordinator.getTabletMap(tabletMap);
    }
  private:
    CoordinatorClient& coordinator;
};

} // anonymous namespace

/**
 * Constructor.
 * \param coordinator
 *      This object keeps a reference to \a coordinator
 */
ObjectFinder::ObjectFinder(CoordinatorClient& coordinator)
    : tabletMap()
    , tabletMapFetcher(new RealTabletMapFetcher(coordinator))
{
}

/**
 * Lookup the master for a particular key in a given table.
 *
 * \param table
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::lookup(uint64_t table, const char* key, uint16_t keyLength) {
    HashType keyHash = getKeyHash(key, keyLength);

    /*
    * The control flow in here is a bit tricky:
    * Since tabletMap is a cache of the coordinator's tablet map, we can only
    * throw TableDoesntExistException if the table doesn't exist after
    * refreshing that cache.
    * Moreover, if the tablet turns out to be in a state of recovery, we have
    * to spin until it is recovered.
    */
    bool haveRefreshed = false;
    while (true) {
        foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
            if (tablet.table_id() == table &&
                tablet.start_key_hash() <= keyHash &&
                keyHash <= tablet.end_key_hash()) {
                if (tablet.state() == ProtoBuf::Tablets_Tablet_State_NORMAL) {
                    // TODO(ongaro): add cache
                    return Context::get().transportManager->getSession(
                            tablet.service_locator().c_str());
                } else {
                    // tablet is recovering or something, try again
                    if (haveRefreshed)
                        usleep(10000);
                    goto refresh_and_retry;
                }
            }
        }
        // tablet not found in local tablet map cache
        if (haveRefreshed) {
            throw TableDoesntExistException(HERE);
        }
        refresh_and_retry:
            tabletMapFetcher->getTabletMap(tabletMap);
            haveRefreshed = true;
    }
}

Transport::SessionRef
ObjectFinder::getSessionRef(std::string serverConnectionString) {
    return Context::get().transportManager->getSession(
                                serverConnectionString.c_str());
}


/**
 * Lookup the masters for multiple keys across tables.
 * \param requests
 *      Array listing the objects to be read/written
 * \param numRequests
 *      Length of requests array
 * \return requestBins
 *      Bins requests according to the master they correspond to.
 */

std::vector<ObjectFinder::MasterRequests>
ObjectFinder::multiLookup(MasterClient::ReadObject* requests[],
                          uint32_t numRequests) {

    std::vector<ObjectFinder::MasterRequests> requestBins;
    for (uint32_t i = 0; i < numRequests; i++){
        try {
            Transport::SessionRef currentSessionRef =
                ObjectFinder::lookup(requests[i]->tableId,
                                     requests[i]->key, requests[i]->keyLength);

            // if this master already exists in the requestBins, add request
            // to the requestBin corresponding to that master
            bool masterFound = false;
            for (uint32_t j = 0; j < requestBins.size(); j++){
                if (currentSessionRef == requestBins[j].sessionRef){
                    requestBins[j].requests.push_back(requests[i]);
                    masterFound = true;
                    break;
                }
            }
            // else create a new requestBin corresponding to this master
            if (!masterFound) {
                requestBins.push_back(ObjectFinder::MasterRequests());
                requestBins.back().sessionRef = currentSessionRef;
                requestBins.back().requests.push_back(requests[i]);
            }
        }
        catch (TableDoesntExistException &e) {
            requests[i]->status = STATUS_TABLE_DOESNT_EXIST;
        }
    }

    return requestBins;
}

std::set<Transport::SessionRef>
ObjectFinder::tableLookup(uint64_t table)
{
    std::set<Transport::SessionRef> sessionRefBins;

    if (sessionRefBins.size() == 0) {
                    // tablet is recovering or something, try again
                    tabletMapFetcher->getTabletMap(tabletMap);
    }


    foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
        if (tablet.table_id() == table) {
            if (tablet.state() == ProtoBuf::Tablets_Tablet_State_NORMAL) {
                // TODO(ongaro): add cache
                sessionRefBins.insert(Context::get().transportManager->getSession(
                                                                tablet.service_locator().c_str()));
            }
        }

    }

    return sessionRefBins;
}

std::vector<ObjectFinder::KeysAtServer>
ObjectFinder::resolveTableDistribution(uint64_t tableId, uint64_t maxKey)
{
    std::vector<ObjectFinder::KeysAtServer> tableDistribution;

    // This map contains all keys and their hash values
    std::map<uint64_t, HashType> keysMap;
    std::map<uint64_t, HashType>::iterator it;

    for(uint64_t i = 0;i<=maxKey;i++) {
        MakeKey key(i);
        HashType keyHash = getKeyHash(key.get(), key.length());
        keysMap[i] = keyHash;
    }

    // Iterate over tablets, check for each tablet which keys are included
    tabletMapFetcher->getTabletMap(tabletMap);

    foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
        if (tablet.table_id() == tableId) {
            if (tablet.state() == ProtoBuf::Tablets_Tablet_State_NORMAL) {

                std::string currentConnectionString =
                                tablet.service_locator().c_str();

                // Check if server is already listed. If yes add keys otherwise
                // add new KeysAtServer
                bool masterFound = false;
                for (uint32_t i = 0; i < tableDistribution.size(); i++) {
                     if (currentConnectionString ==
                                tableDistribution[i].serverConnectionString) {

                       for (it=keysMap.begin(); it != keysMap.end(); it++) {
                            if (tablet.start_key_hash() <= it->second &&
                                it->second <= tablet.end_key_hash()) {
                                tableDistribution[i].keys.push_back(it->first);
                            }
                        }

                       masterFound = true;
                       break;
                     }
                }

                if (!masterFound) {
                    tableDistribution.push_back(ObjectFinder::KeysAtServer());
                    tableDistribution.back().serverConnectionString =
                                        tablet.service_locator().c_str();

                    for (it=keysMap.begin(); it != keysMap.end(); it++) {
                         if (tablet.start_key_hash() <= it->second &&
                             it->second <= tablet.end_key_hash()) {
                             tableDistribution.back().keys.push_back(it->first);
                         }
                    }
                }

            }
        }

    }

    return tableDistribution;
}



/**
 * Flush the tablet map and refresh it until we detect that at least one tablet
 * has a state set to something other than normal.
 *
 * Used only by RecoveryMain.c to detect when the failure is detected by the
 * coordinator.
 */
void
ObjectFinder::waitForTabletDown()
{
    flush();

    for (;;) {
        foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
            if (tablet.state() != ProtoBuf::Tablets_Tablet_State_NORMAL) {
                return;
            }
        }
        usleep(200);
        tabletMapFetcher->getTabletMap(tabletMap);
    }
}


/**
 * Flush the tablet map and refresh it until it is non-empty and all of
 * the tablets have normal status.
 *
 * Used only by RecoveryMain.c to detect when the recovery is complete.
 */
void
ObjectFinder::waitForAllTabletsNormal()
{
    flush();

    for (;;) {
        bool allNormal = true;
        foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
            if (tablet.state() != ProtoBuf::Tablets_Tablet_State_NORMAL) {
                allNormal = false;
                break;
            }
        }
        if (allNormal && tabletMap.tablet_size() > 0)
            return;
        usleep(200);
        tabletMapFetcher->getTabletMap(tabletMap);
    }
}


} // namespace RAMCloud
