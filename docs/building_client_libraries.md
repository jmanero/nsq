## Building Client Libraries

This document is intended to provide a language-agnostic baseline of expected functionality
and behavior for client library implementations.

By setting these expectations we hope to provide a foundation for achieving consistency across
languages for NSQ users.

For details on the low-level TCP protocol, see the [protocol spec](protocol.md).

### Overview

An NSQ client can either produce or consume messages.  For consumers, the focal points are:

 1. Configuration
 2. Discovery (optional)
 3. Connection Handling
 4. Data Flow / Heartbeats
 5. Message Handling
 6. RDY State Management
 7. Backoff

### Configuration

At a high level, our philosophy with respect to configuration is to design the system to have the
flexibility to support different workloads, use sane defaults that run well "out of the box", and
minimize the number of dials.

A client subscribes to a `topic` on a `channel` over a TCP connection to `nsqd` instance(s). You can
only subscribe to one topic per connection so multiple topic consumption needs to be structured
accordingly.

Using `nsqlookupd` is optional so client libraries should support a configuration where a client
connects *directly* to one or more `nsqd` instances or where it is configured to poll one or more
`nsqlookupd` instances. When a client is configured to poll `nsqlookupd` the polling interval should
be configurable. Additionally, because typical deployments of NSQ are in distributed environments
with many producers and consumers, the client library should automatically add jitter based on a
random % of the configured value. This will help avoid a thundering herd of connections. For more
detail see [Discovery](#discovery).

An important performance knob for clients is the number of messages it can receive before `nsqd`
expects a response. Think of this as pipelining which facilitates buffered, batched, and
asynchronous message handling. By convention this is called `max_in_flight` and it effects how `RDY`
state is managed. For more detail see [RDY State Management](#rdy_state_management).

Being a system that is designed to gracefully handle failure, client libraries are expected to 
implement retry handling for failed messages and provide options for bounding that behavior in terms
of number of attempts per message.  For more detail see [Message Handling](#message_handling).

Relatedly, when message processing fails, the client library is expected to automatically handle
re-queueing the message. NSQ supports sending a delay along with the `REQ` command. Client libraries
are expected to provide options for what this delay should be set to initially (for the first
failure) and how it should change for subsequent failures. For more detail see [Backoff](#backoff).

Most importantly, the client library should support some method of configuring callback handlers for
message processing. The signature of these callbacks should be simple, typically just an instance of
a "message object".

### Discovery

An important component of NSQ is `nsqlookupd`.  It provides a discovery service for consumers to
locate producers of a given topic at runtime.

When a client uses `nsqlookupd` for discovery, the client library should manage the process of
polling all `nsqlookupd` instances as well as managing connections to the producers that are
returned.

Querying an `nsqlookupd` instance is straightforward. Perform an HTTP request to the lookup endpoint
with a query parameter of the topic the client is attempting to discover (ie.
`/lookup?topic=clicks`).  The response format is JSON:

```json
{
    "status_code": 200,
    "status_txt": "OK",
    "data": {
        "channels": ["archive", "science", "metrics"],
        "producers": [
            {
                "broadcast_address": "clicksapi01.routable.domain.net", 
                "hostname": "clicksapi01.domain.net",
                "tcp_port": 4150,
                "http_port": 4151,
                "version": "0.2.18"
            },
            {
                "broadcast_address": "clicksapi02.routable.domain.net", 
                "hostname": "clicksapi02.domain.net",
                "tcp_port": 4150,
                "http_port": 4151,
                "version": "0.2.18"
            }
        ]
    }
}
```

For consumer connections, the `broadcast_address` and `tcp_port` should be used. By design,
`nsqlookupd` instances don't coordinate (`nsqd` pushes metadata to all of them). The client library
should union the list of producers from all payloads to build the final list of `nsqd` instances to
connect to.

A periodic timer should be used to repeatedly poll the configured `nsqlookupd` (and connect) so that
clients will automatically discover new producers.

When the client library execution begins it should bootstrap this polling process by kicking off an
initial set of requests to the configured instances.

### Connection Handling

After discovery (or when configured with specific `nsqd` instances to connect to) the client library
should open a TCP connection to the `address:port` for each topic the client wants to consume.

More detailed steps on the protocol are available in the [protocol spec](protocol.md) however, in
short, the library should send the following data (in order):

 1. the magic identifier
 2. an `IDENTIFY` command (and payload) and read/verify response
 3. a `SUB` command (specifying desired topic) and read/verify response
 4. an initial `RDY` count of 1 (see [RDY State Management](#rdy_state_management)).

Client libraries should automatically handle reconnection, but the behavior is different
depending on the configuration:

 * If the client is configured with a specific list of `nsqd` instances, reconnection should be
   handled by delaying the retry attempt in an exponential backoff manner (ie. try to reconnect in
   8s, 16s, 32s, etc. up to a max).

 * If the client is configured to discover instances via `nsqlookupd`, reconnection should be
   handled automatically based on the polling interval (ie. if a client disconnects from an `nsqd`,
   the client library should attempt reconnect if that instance is discovered by a subsequent
   `nsqlookupd` polling round). This ensures that clients can learn about producers that are
   introduced to the topology *and* ones that are removed (or failed).

### Data Flow and Heartbeats

Once a client is in a subscribed state, data flow in the NSQ protocol is asynchronous. For
consumers, this means that in order to build truly robust and performant client libraries they
should be structured using asynchronous network IO loops and/or "threads" (the scare quotes are used
to represent both OS threads and user-land threads like co-routines).

Additionally clients are expected to respond to periodic heartbeats from the `nsqd` instances
they're connected to. By default this happens at 30s intervals. The client can respond with *any*
command but, by convention, it's easiest to simply respond with a `NOP` whenever a heartbeat is
received.  See the [protocol spec](protocol.md) for specifics on how to identify heartbeats.

A "thread" should be dedicated to reading data off the TCP socket, unpacking the data from the
frame, and performing the multiplexing logic to route the data as appropriate. This is also
conveniently the best spot to handle heartbeats.

Related to connection handling, the overwhelming majority of protocol level error handling is fatal.
This means that if the client sends an invalid command or gets itself into an invalid state the
`nsqd` instance it is connected to will protect itself (and the system) by forceable closing
the connection (and, if possible, sending the error to the client).

### Message Handling

When the IO loop unpacks a message it should route that message to the configured handler for 
processing.

The `nsqd` instance expects to receive a reply within the server's configured message timeout
(default 60s). There are a few possible scenarios:

 1. The callback indicates that the message was handled successfully.
 2. The callback indicates that the message handling was unsuccessful.
 3. The callback decides that it needs more time to process the message.
 4. The in-flight timeout expires and `nsqd` automatically re-queues the message.

In the first 3 cases, the client library should send the appropriate command on the client's behalf
(`FIN`, `REQ`, and `TOUCH` respectively).

The `FIN` command is the simplest of the 3. It tells `nsqd` that it can discard the message.

The `REQ` command tells `nsqd` that the message should be re-queued (with an optional parameter
specifying the *re-queue delay*). This re-queue delay should grow based on the number of attempts (a
multiple is typically sufficient). The client library should discard messages that exceed the
configured max attempts with some a callback to indicate that this situation occurred (such that the
user can implement special handling for failed messages that are discarded).

If message handling requires more time than the configured message timeout the `TOUCH` command can
be used to reset the timer on the `nsqd` side. This can be done repeatedly until the message is
either `FIN` or `REQ` up to the server's configured max timeout.

Finally, if the `nsqd` instance receives *no* response, the message will time out and be
automatically re-queued for delivery to an available client.

Without digging into language specific implementation, we've found that structuring the above
commands as "instance methods" on the "message object" and that instance be the thing that is passed
to your message handler works best. This object needs to manage enough state to be able to reply
over the corresponding TCP connection.

The benefit of this is that it supports handler code that can be structured in both synchronous and
asynchronous fashion. For trivial processing, being able to simply return a boolean to the client
library indicating success/failure is powerful. Like this example in Python:

```python
import nsq

def message_handler(msg):
    # this method returns True/False and the client library
    # will handle msg.finish() or msg.requeue() automagically
    return process_message(msg.body)

if __name__ == '__main__':
    nsq.Reader(topic='test', channel='test', tasks={'handler_name': message_handler})
    nsq.run()
```

For more complicated asynchronous processing, the API remains the same (again using Python):

```python
import nsq
import functools

def async_message_handler(msg):
    callback = functools.partial(finish_async_http_request, msg=msg)
    async_http_request('http://api.domain.net/endpoint', body=msg.body, callback=callback)

def finish_async_http_request(response, msg):
    if not response:
        msg.requeue()
    else:
        msg.finish()

if __name__ == '__main__':
    nsq.Reader(topic='test', channel='test', tasks={'handler_name': async_message_handler})
    nsq.run()
```

