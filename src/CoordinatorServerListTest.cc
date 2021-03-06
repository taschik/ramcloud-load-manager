/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "CoordinatorServerList.h"

namespace RAMCloud {

class CoordinatorServerListTest : public ::testing::Test {
  public:
    CoordinatorServerListTest()
        : sl()
    {
    }

    CoordinatorServerList sl;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerListTest);
};

/*
 * Return true if a CoordinatorServerList::Entry is indentical to the
 * given serialized protobuf entry.
 */
static bool
protoBufMatchesEntry(const ProtoBuf::ServerList_Entry& protoBufEntry,
                     const CoordinatorServerList::Entry& serverListEntry,
                     ServerStatus status)
{
    if (serverListEntry.serviceMask.serialize() !=
        protoBufEntry.service_mask())
        return false;
    if (*serverListEntry.serverId != protoBufEntry.server_id())
        return false;
    if (serverListEntry.serviceLocator != protoBufEntry.service_locator())
        return false;
    if (serverListEntry.backupReadMBytesPerSec !=
        protoBufEntry.backup_read_mbytes_per_sec())
        return false;
    if (status != ServerStatus(protoBufEntry.status()))
        return false;

    return true;
}

TEST_F(CoordinatorServerListTest, constructor) {
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);
    EXPECT_EQ(0U, sl.versionNumber);
}

TEST_F(CoordinatorServerListTest, add) {
    EXPECT_EQ(0U, sl.serverList.size());
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);

    {
        ProtoBuf::ServerList update1;
        EXPECT_EQ(ServerId(1, 0), sl.add("hi", {MASTER_SERVICE},
                                         100, update1));
        EXPECT_TRUE(sl.serverList[1].entry);
        EXPECT_FALSE(sl.serverList[0].entry);
        EXPECT_EQ(1U, sl.numberOfMasters);
        EXPECT_EQ(0U, sl.numberOfBackups);
        EXPECT_EQ(ServerId(1, 0), sl.serverList[1].entry->serverId);
        EXPECT_EQ("hi", sl.serverList[1].entry->serviceLocator);
        EXPECT_TRUE(sl.serverList[1].entry->isMaster());
        EXPECT_FALSE(sl.serverList[1].entry->isBackup());
        EXPECT_EQ(0u, sl.serverList[1].entry->backupReadMBytesPerSec);
        EXPECT_EQ(1U, sl.serverList[1].nextGenerationNumber);
        EXPECT_EQ(0U, sl.versionNumber);
        sl.incrementVersion(update1);
        EXPECT_EQ(1U, sl.versionNumber);
        EXPECT_EQ(1U, update1.version_number());
        EXPECT_EQ(1, update1.server_size());
        EXPECT_TRUE(protoBufMatchesEntry(update1.server(0),
            *sl.serverList[1].entry, ServerStatus::UP));
    }

    {
        ProtoBuf::ServerList update2;
        EXPECT_EQ(ServerId(2, 0), sl.add("hi again",
                                         {BACKUP_SERVICE}, 100, update2));
        EXPECT_TRUE(sl.serverList[2].entry);
        EXPECT_EQ(ServerId(2, 0), sl.serverList[2].entry->serverId);
        EXPECT_EQ("hi again", sl.serverList[2].entry->serviceLocator);
        EXPECT_FALSE(sl.serverList[2].entry->isMaster());
        EXPECT_TRUE(sl.serverList[2].entry->isBackup());
        EXPECT_EQ(100u, sl.serverList[2].entry->backupReadMBytesPerSec);
        EXPECT_EQ(1U, sl.serverList[2].nextGenerationNumber);
        EXPECT_EQ(1U, sl.numberOfMasters);
        EXPECT_EQ(1U, sl.numberOfBackups);
        EXPECT_EQ(1U, sl.versionNumber);
        sl.incrementVersion(update2);
        EXPECT_EQ(2U, sl.versionNumber);
        EXPECT_EQ(2U, update2.version_number());
        EXPECT_TRUE(protoBufMatchesEntry(update2.server(0),
            *sl.serverList[2].entry, ServerStatus::UP));
    }
}

TEST_F(CoordinatorServerListTest, crashed) {
    ProtoBuf::ServerList update;

    EXPECT_THROW(sl.crashed(ServerId(0, 0), update), Exception);
    EXPECT_EQ(0, update.server_size());

    sl.add("hi!", {MASTER_SERVICE}, 100, update);
    CoordinatorServerList::Entry entryCopy = sl[ServerId(1, 0)];
    update.Clear();
    EXPECT_NO_THROW(sl.crashed(ServerId(1, 0), update));
    ASSERT_TRUE(sl.serverList[1].entry);
    EXPECT_EQ(ServerStatus::CRASHED, sl.serverList[1].entry->status);
    EXPECT_TRUE(protoBufMatchesEntry(update.server(0),
                                     entryCopy, ServerStatus::CRASHED));

    update.Clear();
    // Already crashed; a no-op.
    sl.crashed(ServerId(1, 0), update);
    EXPECT_EQ(0, update.server_size());
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);
}

TEST_F(CoordinatorServerListTest, remove) {
    ProtoBuf::ServerList addUpdate, removeUpdate;

    EXPECT_THROW(sl.remove(ServerId(0, 0), removeUpdate), Exception);
    EXPECT_EQ(0, removeUpdate.server_size());

    sl.add("hi!", {MASTER_SERVICE}, 100, addUpdate);
    CoordinatorServerList::Entry entryCopy = sl[ServerId(1, 0)];
    EXPECT_NO_THROW(sl.remove(ServerId(1, 0), removeUpdate));
    EXPECT_FALSE(sl.serverList[1].entry);
    EXPECT_TRUE(protoBufMatchesEntry(removeUpdate.server(0),
            entryCopy, ServerStatus::CRASHED));
    EXPECT_TRUE(protoBufMatchesEntry(removeUpdate.server(1),
            entryCopy, ServerStatus::DOWN));

    EXPECT_THROW(sl.remove(ServerId(1, 0), removeUpdate), Exception);
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);

    removeUpdate.Clear();
    sl.add("hi, again", {BACKUP_SERVICE}, 100, addUpdate);
    sl.crashed(ServerId(1, 1), addUpdate);
    EXPECT_TRUE(sl.serverList[1].entry);
    EXPECT_THROW(sl.remove(ServerId(1, 2), removeUpdate), Exception);
    EXPECT_NO_THROW(sl.remove(ServerId(1, 1), removeUpdate));
    EXPECT_EQ(uint32_t(ServerStatus::DOWN), removeUpdate.server(0).status());
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);
}

TEST_F(CoordinatorServerListTest, incrementVersion) {
    ProtoBuf::ServerList update;
    sl.incrementVersion(update);
    EXPECT_EQ(1u, sl.versionNumber);
    EXPECT_EQ(1u, update.version_number());
}

TEST_F(CoordinatorServerListTest, indexOperator) {
    ProtoBuf::ServerList update;
    EXPECT_THROW(sl[ServerId(0, 0)], Exception);
    sl.add("yo!", {MASTER_SERVICE}, 100, update);
    EXPECT_EQ(ServerId(1, 0), sl[ServerId(1, 0)].serverId);
    EXPECT_EQ("yo!", sl[ServerId(1, 0)].serviceLocator);
    sl.crashed(ServerId(1, 0), update);
    sl.remove(ServerId(1, 0), update);
    EXPECT_THROW(sl[ServerId(1, 0)], Exception);
}

TEST_F(CoordinatorServerListTest, contains) {
    ProtoBuf::ServerList update;

    EXPECT_FALSE(sl.contains(ServerId(0, 0)));
    EXPECT_FALSE(sl.contains(ServerId(1, 0)));

    sl.add("I love it when a plan comes together",
           {BACKUP_SERVICE}, 100, update);
    EXPECT_TRUE(sl.contains(ServerId(1, 0)));

    sl.add("Come with me if you want to live",
           {MASTER_SERVICE}, 100, update);
    EXPECT_TRUE(sl.contains(ServerId(2, 0)));

    sl.crashed(ServerId(1, 0), update);
    EXPECT_TRUE(sl.contains(ServerId(1, 0)));
    sl.remove(ServerId(1, 0), update);
    EXPECT_FALSE(sl.contains(ServerId(1, 0)));

    sl.crashed(ServerId(2, 0), update);
    sl.remove(ServerId(2, 0), update);
    EXPECT_FALSE(sl.contains(ServerId(2, 0)));

    sl.add("I'm running out 80s shows and action movie quotes",
           {BACKUP_SERVICE}, 100, update);
    EXPECT_TRUE(sl.contains(ServerId(1, 1)));
}

TEST_F(CoordinatorServerListTest, nextMasterIndex) {
    ProtoBuf::ServerList update;

    EXPECT_EQ(-1U, sl.nextMasterIndex(0));
    sl.add("", {BACKUP_SERVICE}, 100, update);
    sl.add("", {MASTER_SERVICE}, 100, update);
    sl.add("", {BACKUP_SERVICE}, 100, update);
    sl.add("", {BACKUP_SERVICE}, 100, update);
    sl.add("", {MASTER_SERVICE}, 100, update);
    sl.add("", {BACKUP_SERVICE}, 100, update);

    EXPECT_EQ(2U, sl.nextMasterIndex(0));
    EXPECT_EQ(2U, sl.nextMasterIndex(2));
    EXPECT_EQ(5U, sl.nextMasterIndex(3));
    EXPECT_EQ(-1U, sl.nextMasterIndex(6));
}

TEST_F(CoordinatorServerListTest, nextBackupIndex) {
    ProtoBuf::ServerList update;

    EXPECT_EQ(-1U, sl.nextMasterIndex(0));
    sl.add("", {MASTER_SERVICE}, 100, update);
    sl.add("", {BACKUP_SERVICE}, 100, update);
    sl.add("", {MASTER_SERVICE}, 100, update);

    EXPECT_EQ(2U, sl.nextBackupIndex(0));
    EXPECT_EQ(2U, sl.nextBackupIndex(2));
    EXPECT_EQ(-1U, sl.nextBackupIndex(3));
}

TEST_F(CoordinatorServerListTest, serialize) {
    ProtoBuf::ServerList update;

    {
        ProtoBuf::ServerList serverList;
        sl.serialize(serverList, {});
        EXPECT_EQ(0, serverList.server_size());
        sl.serialize(serverList, {MASTER_SERVICE, BACKUP_SERVICE});
        EXPECT_EQ(0, serverList.server_size());
    }

    ServerId first = sl.add("", {MASTER_SERVICE}, 100, update);
    sl.add("", {MASTER_SERVICE}, 100, update);
    sl.add("", {MASTER_SERVICE}, 100, update);
    sl.add("", {BACKUP_SERVICE}, 100, update);
    sl.remove(first, update);       // ensure removed entries are skipped

    auto masterMask = ServiceMask{MASTER_SERVICE}.serialize();
    auto backupMask = ServiceMask{BACKUP_SERVICE}.serialize();
    {
        ProtoBuf::ServerList serverList;
        sl.serialize(serverList, {});
        EXPECT_EQ(0, serverList.server_size());
        sl.serialize(serverList, {MASTER_SERVICE});
        EXPECT_EQ(2, serverList.server_size());
        EXPECT_EQ(masterMask, serverList.server(0).service_mask());
        EXPECT_EQ(masterMask, serverList.server(1).service_mask());
    }

    {
        ProtoBuf::ServerList serverList;
        sl.serialize(serverList, {BACKUP_SERVICE});
        EXPECT_EQ(1, serverList.server_size());
        EXPECT_EQ(backupMask, serverList.server(0).service_mask());
    }

    {
        ProtoBuf::ServerList serverList;
        sl.serialize(serverList, {MASTER_SERVICE, BACKUP_SERVICE});
        EXPECT_EQ(3, serverList.server_size());
        EXPECT_EQ(masterMask, serverList.server(0).service_mask());
        EXPECT_EQ(masterMask, serverList.server(1).service_mask());
        EXPECT_EQ(backupMask, serverList.server(2).service_mask());
    }
}

TEST_F(CoordinatorServerListTest, firstFreeIndex) {
    ProtoBuf::ServerList update;

    EXPECT_EQ(0U, sl.serverList.size());
    EXPECT_EQ(1U, sl.firstFreeIndex());
    EXPECT_EQ(2U, sl.serverList.size());
    sl.add("hi", {MASTER_SERVICE}, 100, update);
    EXPECT_EQ(2U, sl.firstFreeIndex());
    sl.add("hi again", {MASTER_SERVICE}, 100, update);
    EXPECT_EQ(3U, sl.firstFreeIndex());
    sl.remove(ServerId(2, 0), update);
    EXPECT_EQ(2U, sl.firstFreeIndex());
    sl.remove(ServerId(1, 0), update);
    EXPECT_EQ(1U, sl.firstFreeIndex());
}

TEST_F(CoordinatorServerListTest, getReferenceFromServerId) {
    ProtoBuf::ServerList update;

    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(0, 0)), Exception);
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(1, 0)), Exception);

    sl.add("", {MASTER_SERVICE}, 100, update);
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(0, 0)), Exception);
    EXPECT_NO_THROW(sl.getReferenceFromServerId(ServerId(1, 0)));
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(2, 0)), Exception);
}

TEST_F(CoordinatorServerListTest, getPointerFromIndex) {
    ProtoBuf::ServerList update;

    EXPECT_THROW(sl.getPointerFromIndex(0), Exception);
    EXPECT_THROW(sl.getPointerFromIndex(1), Exception);

    sl.add("", {MASTER_SERVICE}, 100, update);
    EXPECT_EQ(static_cast<const CoordinatorServerList::Entry*>(NULL),
        sl.getPointerFromIndex(0));
    EXPECT_EQ(static_cast<const CoordinatorServerList::Entry*>(
        sl.serverList[1].entry.get()), sl.getPointerFromIndex(1));
    EXPECT_THROW(sl.getPointerFromIndex(2), Exception);

    sl.remove(ServerId(1, 0), update);
    EXPECT_EQ(static_cast<const CoordinatorServerList::Entry*>(NULL),
        sl.getPointerFromIndex(1));
}

TEST_F(CoordinatorServerListTest, Entry_constructor) {
    CoordinatorServerList::Entry a(ServerId(52, 374),
        "You forgot your boarding pass", {MASTER_SERVICE});
    EXPECT_EQ(ServerId(52, 374), a.serverId);
    EXPECT_EQ("You forgot your boarding pass", a.serviceLocator);
    EXPECT_TRUE(a.isMaster());
    EXPECT_FALSE(a.isBackup());
    EXPECT_EQ(static_cast<ProtoBuf::Tablets*>(NULL), a.will);
    EXPECT_EQ(0U, a.backupReadMBytesPerSec);

    CoordinatorServerList::Entry b(ServerId(27, 72),
        "I ain't got time to bleed", {BACKUP_SERVICE});
    EXPECT_EQ(ServerId(27, 72), b.serverId);
    EXPECT_EQ("I ain't got time to bleed", b.serviceLocator);
    EXPECT_FALSE(b.isMaster());
    EXPECT_TRUE(b.isBackup());
    EXPECT_EQ(static_cast<ProtoBuf::Tablets*>(NULL), b.will);
    EXPECT_EQ(0U, b.backupReadMBytesPerSec);
}

static bool
compareEntries(CoordinatorServerList::Entry& a,
               CoordinatorServerList::Entry& b)
{
    // If this trips, you need to update some checks below.
    EXPECT_EQ(56U, sizeof(a));

    if (a.serverId != b.serverId)
        return false;
    if (a.serviceLocator != b.serviceLocator)
        return false;
    if (a.isMaster() != b.isMaster())
        return false;
    if (a.isBackup() != b.isBackup())
        return false;
    if (a.will != b.will)
        return false;
    if (a.backupReadMBytesPerSec != b.backupReadMBytesPerSec)
        return false;
    if (a.minOpenSegmentId != b.minOpenSegmentId)
        return false;
    if (a.replicationId != b.replicationId)
        return false;

    return true;
}

TEST_F(CoordinatorServerListTest, Entry_copyConstructor) {
    CoordinatorServerList::Entry source(
        ServerId(234, 273), "hi!", {BACKUP_SERVICE});
    source.backupReadMBytesPerSec = 57;
    source.will = reinterpret_cast<ProtoBuf::Tablets*>(0x11deadbeef22UL);
    CoordinatorServerList::Entry dest(source);
    EXPECT_TRUE(compareEntries(source, dest));
}

TEST_F(CoordinatorServerListTest, Entry_assignmentOperator) {
    CoordinatorServerList::Entry source(ServerId(73, 72), "hi",
                                        {BACKUP_SERVICE});
    source.backupReadMBytesPerSec = 785;
    source.will = reinterpret_cast<ProtoBuf::Tablets*>(0x11beefcafe22UL);
    CoordinatorServerList::Entry dest(ServerId(0, 0), "bye",
                                      {MASTER_SERVICE});
    dest = source;
    EXPECT_TRUE(compareEntries(source, dest));
}

TEST_F(CoordinatorServerListTest, Entry_serialize) {
    CoordinatorServerList::Entry entry(ServerId(0, 0), "",
                                       {BACKUP_SERVICE});
    entry.serverId = ServerId(5234, 23482);
    entry.serviceLocator = "giggity";
    entry.backupReadMBytesPerSec = 723;

    ProtoBuf::ServerList_Entry serialEntry;
    entry.serialize(serialEntry);
    auto backupMask = ServiceMask{BACKUP_SERVICE}.serialize();
    EXPECT_EQ(backupMask, serialEntry.service_mask());
    EXPECT_EQ(ServerId(5234, 23482).getId(), serialEntry.server_id());
    EXPECT_EQ("giggity", serialEntry.service_locator());
    EXPECT_EQ(723U, serialEntry.backup_read_mbytes_per_sec());
    EXPECT_EQ(ServerStatus::UP, ServerStatus(serialEntry.status()));

    entry.serviceMask = ServiceMask{MASTER_SERVICE};
    ProtoBuf::ServerList_Entry serialEntry2;
    entry.serialize(serialEntry2);
    auto masterMask = ServiceMask{MASTER_SERVICE}.serialize();
    EXPECT_EQ(masterMask, serialEntry2.service_mask());
    EXPECT_EQ(0U, serialEntry2.backup_read_mbytes_per_sec());
    EXPECT_EQ(ServerStatus::UP, ServerStatus(serialEntry2.status()));
}

}  // namespace RAMCloud
