/* Copyright (c) 2011 Stanford University
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

/**
 * \file
 * This file implements the CoordinatorServerList class.
 */

#include "Common.h"
#include "CoordinatorServerList.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Public Methods
//////////////////////////////////////////////////////////////////////

/**
 * Constructor for CoordinatorServerList.
 */
CoordinatorServerList::CoordinatorServerList()
    : serverList(),
      numberOfMasters(0),
      numberOfBackups(0),
      versionNumber(0)
{
}

/**
 * Destructor for CoordinatorServerList.
 */
CoordinatorServerList::~CoordinatorServerList()
{
}

/**
 * Add a new server to the CoordinatorServerList and generate a new, unique
 * ServerId for it.
 *
 * After an add() but before sending \a update to the cluster
 * incrementVersion() must be called.  Also, \a update can contain remove,
 * crash, and add notifications, but removals/crashes must precede additions
 * in the update to ensure ordering guarantees about notifications related to
 * servers which re-enlist.  For now, this means calls to remove()/crashed()
 * must proceed call to add() if they have a common \a update.
 *
 * \param serviceLocator
 *      The ServiceLocator string of the server to add.
 * \param serviceMask
 *      Which services this server supports.
 *  \param readSpeed
 *      Speed of the storage on the enlisting server if it includes a backup
 *      service.  Argument is ignored otherwise.
 * \param update
 *      Cluster membership update message to append a serialized add
 *      notification to.
 * \return
 *      The unique ServerId assigned to this server.
 */
ServerId
CoordinatorServerList::add(string serviceLocator,
                           ServiceMask serviceMask,
                           uint32_t readSpeed,
                           ProtoBuf::ServerList& update)
{
    uint32_t index = firstFreeIndex();

    ServerId id(index, serverList[index].nextGenerationNumber);
    serverList[index].nextGenerationNumber++;
    serverList[index].entry.construct(id, serviceLocator, serviceMask);

    if (serviceMask.has(MASTER_SERVICE))
        numberOfMasters++;
    if (serviceMask.has(BACKUP_SERVICE)) {
        numberOfBackups++;
        serverList[index].entry->backupReadMBytesPerSec = readSpeed;
    }

    ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
    serverList[index].entry->serialize(protoBufEntry);

    return id;
}

/**
 * Mark a server as crashed in the list (when it has crashed and is
 * being recovered and resources [replicas] for its recovery must be
 * retained).
 *
 * This is a no-op of the server is already marked as crashed;
 * the effect is undefined if the server's status is DOWN.
 *
 * After a crashed() but before sending \a update to the cluster
 * incrementVersion() must be called.  Also, \a update can contain remove,
 * crash, and add notifications, but removals/crashes must precede additions
 * in the update to ensure ordering guarantees about notifications related to
 * servers which re-enlist.  For now, this means calls to remove()/crashed()
 * must proceed call to add() if they have a common \a update.
 *
 * \param serverId
 *      The ServerId of the server to remove from the CoordinatorServerList.
 *      It must not have been removed already (see remove()).
 * \param update
 *      Cluster membership update message to append a serialized crash
 *      notification to.
 */
void
CoordinatorServerList::crashed(ServerId serverId,
                               ProtoBuf::ServerList& update)
{
    uint32_t index = serverId.indexNumber();
    if (index >= serverList.size() || !serverList[index].entry ||
        serverList[index].entry->serverId != serverId) {
        throw Exception(HERE,
                        format("Invalid ServerId (%lu)", serverId.getId()));
    }

    if (serverList[index].entry->status == ServerStatus::CRASHED)
        return;
    assert(serverList[index].entry->status != ServerStatus::DOWN);

    if (serverList[index].entry->isMaster())
        numberOfMasters--;
    if (serverList[index].entry->isBackup())
        numberOfBackups--;

    serverList[index].entry->status = ServerStatus::CRASHED;

    ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
    serverList[index].entry->serialize(protoBufEntry);
}

/**
 * Remove a server from the list, typically when it is no longer part of
 * the system and we don't care about it anymore (it crashed and has
 * been properly recovered).
 *
 * This method may actually append two entries to \a update (see below).
 *
 * After a remove() but before sending \a update to the cluster
 * incrementVersion() must be called.  Also, \a update can contain remove,
 * crash, and add notifications, but removals/crashes must precede additions
 * in the update to ensure ordering guarantees about notifications related to
 * servers which re-enlist.  For now, this means calls to remove()/crashed()
 * must proceed call to add() if they have a common \a update.
 *
 * \param serverId
 *      The ServerId of the server to remove from the CoordinatorServerList.
 *      It must be in the list (either UP or CRASHED).
 * \param update
 *      Cluster membership update message to append a serialized removal
 *      notification to.  A crash notification will be appended before the
 *      removal notification if this server was removed while in UP status.
 */
void
CoordinatorServerList::remove(ServerId serverId,
                              ProtoBuf::ServerList& update)
{
    uint32_t index = serverId.indexNumber();
    if (index >= serverList.size() || !serverList[index].entry ||
        serverList[index].entry->serverId != serverId) {
        throw Exception(HERE,
                        format("Invalid ServerId (%lu)", serverId.getId()));
    }

    crashed(serverId, update);

    // Even though we destroy this entry almost immediately setting the state
    // gets the serialized update message's state field correct.
    serverList[index].entry->status = ServerStatus::DOWN;

    ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
    serverList[index].entry->serialize(protoBufEntry);
    serverList[index].entry.destroy();
}

/**
 * Increments the list's version number and sets the version number on
 * \a update to match, this must be called after remove()/add() calls have
 * changed the list but before the update message has been sent to the
 * cluster members.
 */
void
CoordinatorServerList::incrementVersion(ProtoBuf::ServerList& update)
{
    versionNumber++;
    update.set_version_number(versionNumber);
}

/**
 * Open a session to the given ServerId. This method simply calls through to
 * TransportManager::getSession. See the documentation there for exceptions
 * that may be thrown.
 *
 * \throw CoordinatorServerListException
 *      A CoordinatorServerListException is thrown if the given ServerId is not
 *      in this list.
 */
Transport::SessionRef
CoordinatorServerList::getSession(ServerId id) const
{
    return Context::get().transportManager->getSession(
        getReferenceFromServerId(id).serviceLocator.c_str(), id);
}

/**
 * \copydetails CoordinatorServerList::getReferenceFromServerId
 */
const CoordinatorServerList::Entry&
CoordinatorServerList::operator[](const ServerId& serverId) const
{
    return getReferenceFromServerId(serverId);
}

/**
 * \copydetails CoordinatorServerList::getReferenceFromServerId
 */
CoordinatorServerList::Entry&
CoordinatorServerList::operator[](const ServerId& serverId)
{
    return const_cast<Entry&>(getReferenceFromServerId(serverId));
}

/**
 * \copydetails CoordinatorServerList::getPointerFromIndex
 */
const CoordinatorServerList::Entry*
CoordinatorServerList::operator[](size_t index) const
{
    return getPointerFromIndex(index);
}

/**
 * \copydetails CoordinatorServerList::getPointerFromIndex
 */
CoordinatorServerList::Entry*
CoordinatorServerList::operator[](size_t index)
{
    return const_cast<Entry*>(getPointerFromIndex(index));
}

/**
 * Return true if the given serverId is in this list regardless of
 * whether it is crashed or not.  This can be used to check membership,
 * rather than having to try and catch around the index operator.
 */
bool
CoordinatorServerList::contains(ServerId serverId) const
{
    if (!serverId.isValid())
        return false;

    uint32_t index = serverId.indexNumber();

    if (index >= serverList.size())
        return false;

    if (!serverList[index].entry)
        return false;

    return serverList[index].entry->serverId == serverId;
}

/**
 * Return the number of valid indexes in this list. Valid does not mean that
 * they're occupied, only that they are within the bounds of the array.
 */
size_t
CoordinatorServerList::size() const
{
    return serverList.size();
}

/**
 * Get the number of masters in the list; does not include servers in
 * crashed status.
 */
uint32_t
CoordinatorServerList::masterCount() const
{
    return numberOfMasters;
}

/**
 * Get the number of backups in the list; does not include servers in
 * crashed status.
 */
uint32_t
CoordinatorServerList::backupCount() const
{
    return numberOfBackups;
}

/**
 * Returns the next index greater than or equal to the given index
 * that describes a master server in the list; masters in crashed
 * status are not returned. If there is no next master or startIndex
 * exceeds the list size, -1 is returned.
 */
uint32_t
CoordinatorServerList::nextMasterIndex(uint32_t startIndex) const
{
    for (; startIndex < serverList.size(); startIndex++) {
        uint32_t i = startIndex;
        if (serverList[i].entry && serverList[i].entry->isMaster())
            break;
    }
    return (startIndex >= serverList.size()) ? -1 : startIndex;
}

/**
 * Returns the next index greater than or equal to the given index
 * that describes a backup server in the list; backups in crashed
 * status are not returned.  If there is no next backup or startIndex
 * exceeds the list size, -1 is returned.
 */
uint32_t
CoordinatorServerList::nextBackupIndex(uint32_t startIndex) const
{
    for (; startIndex < serverList.size(); startIndex++) {
        uint32_t i = startIndex;
        if (serverList[i].entry && serverList[i].entry->isBackup())
            break;
    }
    return (startIndex >= serverList.size()) ? -1 : startIndex;
}

/**
 * Serialise the entire list to a Protocol Buffer form.
 *
 * \param[out] protoBuf
 *      Reference to the ProtoBuf to fill.
 */
void
CoordinatorServerList::serialize(ProtoBuf::ServerList& protoBuf) const
{
    serialize(protoBuf, {MASTER_SERVICE, BACKUP_SERVICE});
}

/**
 * Serialise this list (or part of it, depending on which services the
 * caller wants) to a protocol buffer. Not all state is included, but
 * enough to be useful for disseminating cluster membership information
 * to other servers.
 *
 * \param[out] protoBuf
 *      Reference to the ProtoBuf to fill.
 *
 * \param services
 *      If a server has *any* service included in \a services it will be
 *      included in the serialization; otherwise, it is skipped.
 */
void
CoordinatorServerList::serialize(ProtoBuf::ServerList& protoBuf,
                                 ServiceMask services) const
{
    for (size_t i = 0; i < serverList.size(); i++) {
        if (!serverList[i].entry)
            continue;

        const Entry& entry = *serverList[i].entry;

        if ((entry.isMaster() && services.has(MASTER_SERVICE)) ||
            (entry.isBackup() && services.has(BACKUP_SERVICE))) {
            ProtoBuf::ServerList_Entry& protoBufEntry(*protoBuf.add_server());
            entry.serialize(protoBufEntry);
        }
    }

    protoBuf.set_version_number(versionNumber);
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Private Methods
//////////////////////////////////////////////////////////////////////

/**
 * Return the first free index in the server list. If the list is
 * completely full, resize it and return the next free one.
 *
 * Note that index 0 is reserved. This method must never return it.
 */
uint32_t
CoordinatorServerList::firstFreeIndex()
{
    // Naive, but probably fast enough for a good long while.
    size_t index;
    for (index = 1; index < serverList.size(); index++) {
        if (!serverList[index].entry)
            break;
    }

    if (index >= serverList.size())
        serverList.resize(index + 1);

    assert(index != 0);
    return downCast<uint32_t>(index);
}

/**
 * Obtain a reference to the entry associated with the given ServerId.
 *
 * \param serverId
 *      The ServerId to look up in the list.
 *
 * \throw
 *      An exception is thrown if the given ServerId is not in this list.
 */
const CoordinatorServerList::Entry&
CoordinatorServerList::getReferenceFromServerId(const ServerId& serverId) const
{
    uint32_t index = serverId.indexNumber();
    if (index < serverList.size() && serverList[index].entry)
        return *serverList[index].entry;

    throw Exception(HERE, format("Invalid ServerId (%lu)", serverId.getId()));
}

/**
 * Obtain a pointer to the entry at the given index of the list. This can
 * be used to iterate over the entire list (in conjunction with the #size
 * method), or by . If there is no entry at the given index, NULL is returned.
 *
 * TODO(Rumble): Should this method always return NULL (i.e. not throw if the index
 *      is out of bounds)?.
 *
 * \param index
 *      The index of the entry to return, if there is one.
 *
 * \throw
 *      An exception is thrown if the index exceeds the length of the list.
 */
const CoordinatorServerList::Entry*
CoordinatorServerList::getPointerFromIndex(size_t index) const
{
    if (index >= serverList.size())
        throw Exception(HERE, format("Index beyond array length (%zd)", index));

    if (!serverList[index].entry)
        return NULL;

    return serverList[index].entry.get();
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList::Entry Methods
//////////////////////////////////////////////////////////////////////

/**
 * Construct a new Entry, which contains the data a coordinator
 * needs to maintain about an enlisted server.
 *
 * \param serverId
 *      The ServerId of the server this entry describes.
 *
 * \param serviceLocatorString
 *      The ServiceLocator string that can be used to address this
 *      entry's server.
 *
 * \param serviceMask
 *      Which services this server supports.
 */
CoordinatorServerList::Entry::Entry(ServerId serverId,
                                    string serviceLocatorString,
                                    ServiceMask serviceMask)
    : serverId(serverId),
      serviceLocator(serviceLocatorString),
      serviceMask(serviceMask),
      will(NULL),
      backupReadMBytesPerSec(0),
      status(ServerStatus::UP),
      minOpenSegmentId(0),
      replicationId(0)
{
}

/**
 * Serialise this entry into the given ProtoBuf.
 */
void
CoordinatorServerList::Entry::serialize(ProtoBuf::ServerList_Entry& dest) const
{
    dest.set_service_mask(serviceMask.serialize());
    dest.set_server_id(serverId.getId());
    dest.set_service_locator(serviceLocator);
    dest.set_status(uint32_t(status));
    if (isBackup())
        dest.set_backup_read_mbytes_per_sec(backupReadMBytesPerSec);
    else
        dest.set_backup_read_mbytes_per_sec(0); // Tests expect the field.
}

} // namespace RAMCloud
