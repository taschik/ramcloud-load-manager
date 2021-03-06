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

#include "ClientException.h"
#include "Cycles.h"
#include "MasterService.h"
#include "Memory.h"
#include "Tablets.pb.h"

namespace RAMCloud {

class RecoverSegmentBenchmark {

  public:
    ServerConfig config;
    ServerList serverList;
    MasterService* service;

    RecoverSegmentBenchmark(string logSize, string hashTableSize,
        int numSegments)
        : config(ServerConfig::forTesting())
        , serverList()
        , service(NULL)
    {
        config.localLocator = "bogus";
        config.coordinatorLocator = "bogus";
        config.setLogAndHashTableSize(logSize, hashTableSize);
        config.services = {MASTER_SERVICE};
        config.master.numReplicas = 0;
        service = new MasterService(config, NULL, serverList);
        service->serverId = ServerId(1, 0);
    }

    ~RecoverSegmentBenchmark()
    {
        delete service;
    }

    void
    run(int numSegments, int dataBytes)
    {
        /*
         * Allocate numSegments Segments and fill them up with objects of
         * size dataBytes. These will be the Segments that we recover.
         */
        uint64_t numObjects = 0;
        uint64_t nextKeyVal = 0;
        Segment *segments[numSegments];
        for (int i = 0; i < numSegments; i++) {
            void *p = Memory::xmalloc(HERE, Segment::SEGMENT_SIZE);
            segments[i] = new Segment((uint64_t)0, i, p,
                Segment::SEGMENT_SIZE, NULL);
            while (1) {
                string key = format("%lu", nextKeyVal);
                uint16_t keyLength = downCast<uint16_t>(key.length());

                DECLARE_OBJECT(o, keyLength, dataBytes);
                o->tableId = 0;
                o->version = 0;
                o->keyLength = keyLength;
                memcpy(o->getKeyLocation(), key.c_str(), keyLength);

                SegmentEntryHandle seh =
                    segments[i]->append(LOG_ENTRY_TYPE_OBJ,
                                        o, o->objectLength(dataBytes));

                nextKeyVal++;
                if (seh == NULL)
                    break;
                numObjects++;
            }
            segments[i]->close(NULL);
        }

        /* Update the list of Tablets */
        ProtoBuf::Tablets_Tablet tablet;
        tablet.set_table_id(0);
        tablet.set_start_key_hash(0);
        tablet.set_end_key_hash(~0UL);
        tablet.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet.set_server_id(service->serverId.getId());
        *service->tablets.add_tablet() = tablet;

        /*
         * Now run a fake recovery.
         */
        uint64_t before = Cycles::rdtsc();
        for (int i = 0; i < numSegments; i++) {
            Segment *s = segments[i];
            service->recoverSegment(s->getId(), s->getBaseAddress(),
                s->getCapacity());
        }
        uint64_t ticks = Cycles::rdtsc() - before;

        uint64_t totalObjectBytes = numObjects * dataBytes;
        uint64_t totalSegmentBytes = numSegments * Segment::SEGMENT_SIZE;
        printf("Recovery of %d %dKB Segments with %d byte Objects took %lu "
            "milliseconds\n", numSegments, Segment::SEGMENT_SIZE / 1024,
            dataBytes, RAMCloud::Cycles::toNanoseconds(ticks) / 1000 / 1000);
        printf("Actual total object count: %lu (%lu bytes in Objects, %.2f%% "
            "overhead)\n", numObjects, totalObjectBytes,
            100.0 *
            static_cast<double>(totalSegmentBytes - totalObjectBytes) /
            static_cast<double>(totalSegmentBytes));

        // clean up
        for (int i = 0; i < numSegments; i++) {
            free(const_cast<void *>(segments[i]->getBaseAddress()));
            segments[i]->freeReplicas();
            delete segments[i];
        }
    }

    DISALLOW_COPY_AND_ASSIGN(RecoverSegmentBenchmark);
};

}  // namespace RAMCloud

int
main()
{
    int numSegments = 80;
    int dataBytes[] = { 64, 128, 256, 512, 1024, 2048, 8192, 0 };

    for (int i = 0; dataBytes[i] != 0; i++) {
        printf("==========================\n");
        RAMCloud::RecoverSegmentBenchmark rsb("2048", "10%", numSegments);
        rsb.run(numSegments, dataBytes[i]);
    }

    return 0;
}
