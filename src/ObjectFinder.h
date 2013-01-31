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

#ifndef RAMCLOUD_OBJECTFINDER_H
#define RAMCLOUD_OBJECTFINDER_H

#include <boost/function.hpp>

#include "Common.h"
#include "CoordinatorClient.h"
#include "Transport.h"
#include "MasterClient.h"

namespace RAMCloud {

/**
 * The client uses this class to get session handles to masters.
 */
class ObjectFinder {
  public:
    class TabletMapFetcher; // forward declaration, see full declaration below

    explicit ObjectFinder(CoordinatorClient& coordinator);

    /**
     * A partition (or bin) corresponding to the requests to be sent
     * to one master in a multiRead / multiWrite operation.
     */
    struct MasterRequests {
        MasterRequests() : sessionRef(), requests() {}
        Transport::SessionRef sessionRef;
        std::vector<MasterClient::ReadObject*> requests;
    };

    struct KeysAtServer {
        KeysAtServer() : serverConnectionString(), keys() {}
        std::string serverConnectionString;
        std::vector<uint64_t> keys;
    };

    Transport::SessionRef lookup(uint64_t table, const char* key,
                                 uint16_t keyLength);
    Transport::SessionRef getSessionRef(std::string serverConnectionString);
    std::vector<MasterRequests> multiLookup(MasterClient::ReadObject* input[],
                                            uint32_t numRequests);

    std::set<Transport::SessionRef> tableLookup(uint64_t table);

    Transport::SessionRef serverLookupWithTabletRange(uint64_t table, uint64_t startKey, uint64_t endKey);

    std::vector<KeysAtServer> resolveTableDistribution(uint64_t tableId,
                                                       uint64_t maxKey);


    /**
     * Jettison all tablet map entries forcing a fetch of fresh mappings
     * on subsequent lookups.
     */
    void flush() {
        tabletMap.Clear();
    }

    void waitForTabletDown();
    void waitForAllTabletsNormal();

  PRIVATE:
    /**
     * A cache of the coordinator's tablet map.
     */
    ProtoBuf::Tablets tabletMap;

    /**
     * Update the local tablet map cache. Usually, calling
     * tabletMapFetcher.getTabletMap() is the same as calling
     * coordinator.getTabletMap(tabletMap). During unit tests, however,
     * this is swapped out with a mock implementation.
     */
    std::unique_ptr<ObjectFinder::TabletMapFetcher> tabletMapFetcher;

    DISALLOW_COPY_AND_ASSIGN(ObjectFinder);
};

/**
 * The interface for ObjectFinder::tabletMapFetcher. This is usually set to
 * RealTabletMapFetcher, which is defined in ObjectFinder.cc.
 */
class ObjectFinder::TabletMapFetcher {
  public:
    virtual ~TabletMapFetcher() {}
    /// See CoordinatorClient::getTabletMap.
    virtual void getTabletMap(ProtoBuf::Tablets& tabletMap) = 0;
};

} // end RAMCloud

#endif  // RAMCLOUD_OBJECTFINDER_H
