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

#ifndef RAMCLOUD_SERVICE_H
#define RAMCLOUD_SERVICE_H

#include <algorithm>

#include "Common.h"
#include "ClientException.h"
#include "Buffer.h"
#include "Rpc.h"

namespace RAMCloud {
// There are cross-dependencies between this header file and ServiceManager.h;
// the declaration below is used instead of #including ServiceManager.h to
// break the circularity.
class Worker;

/**
 * Base class for RPC services.  Each service manages a related set of RPC
 * requests, such as those for a master or backup. Although this class is meant
 * to be subclassed, it serves pings so you can use it as a placeholder to
 * aid in development.
 */
class Service {
  public:
    /**
     * Inside a Service an Rpc is represented with this class rather
     * than Transport::ServerRpc.  Most of the member variables in
     * Service::Rpc refer to members in a corresponding Transport::ServerRpc,
     * but having separate classes allows the service to send a reply before
     * it has completed all of its processing: the transport will complete the
     * Transport::ServerRpc and potentially reuse it for a new RPC, and the
     * Service::Rpc keeps track of whether a reply has been sent, which is
     * needed when the service eventually returns.
     */
    class Rpc {
      public:
        /**
         * Constructor for Rpc.
         */
        Rpc(Worker* worker, Buffer& requestPayload,
                Buffer& replyPayload)
            : requestPayload(requestPayload)
            , replyPayload(replyPayload)
            , worker(worker)
            , replied(false) {}

        void sendReply();

        /// The incoming request, which describes the desired operation.
        Buffer& requestPayload;

        /// The response, which will eventually be returned to the client.
        Buffer& replyPayload;

      PRIVATE:
        /// Information about the worker thread that is executing
        /// this request.
        Worker* worker;

        /// True means that sendReply has been invoked.
        bool replied;

        friend class ServiceManager;
        DISALLOW_COPY_AND_ASSIGN(Rpc);
    };

    Service() {}
    virtual ~Service() {}
    virtual void dispatch(RpcOpcode opcode,
                          Rpc& rpc);
    static void prepareErrorResponse(Buffer& buffer, Status status);

    static const char* getString(Buffer& buffer, uint32_t offset,
                                 uint32_t length);
    void handleRpc(Rpc& rpc);

    /**
     * Returns the maximum number of threads that may be executing in
     * this service concurrently.  The default is one, which is for
     * services that are not thread-safe.
     */
    virtual int maxThreads() {
        return 1;
    }

    void ping(const PingRpc::Request& reqHdr,
              PingRpc::Response& respHdr,
              Rpc& rpc);

  PROTECTED:
    /**
     * Helper function for use in dispatch.
     * Extracts the request from the RPC, allocates and zeros space for the
     * response, and calls the handler.
     * \tparam Op
     *      Class associated with a particular operation (e.g. PingRpc).
     * \tparam S
     *      The class that defines \a handler and is a subclass of Service.
     * \tparam handler
     *      The method of \a S which executes an RPC.
     */
    template <typename Op, typename S,
              void (S::*handler)(const typename Op::Request&,
                                 typename Op::Response&,
                                 Rpc&)>
    void
    callHandler(Rpc& rpc) {
        assert(rpc.replyPayload.getTotalLength() == 0);
        const typename Op::Request* reqHdr =
            rpc.requestPayload.getStart<typename Op::Request>();
        if (reqHdr == NULL)
            throw MessageTooShortError(HERE);
        typename Op::Response* respHdr =
            new(&rpc.replyPayload, APPEND) typename Op::Response;
        /* Clear the response header, so that unused fields are zero;
         * this makes tests more reproducible, and it is also needed
         * to avoid possible security problems where random server
         * info could leak out to clients through unused packet
         * fields. */
        memset(respHdr, 0, sizeof(*respHdr));
        (static_cast<S*>(this)->*handler)(*reqHdr, *respHdr, rpc);
    }

  private:
    friend class BindTransport;
    DISALLOW_COPY_AND_ASSIGN(Service);
};


} // end RAMCloud

#endif  // RAMCLOUD_SERVICE_H
