## Building Client Libraries

This document is intended to provide a language-agnostic baseline of expected functionality
and behavior for client library implementations.

By setting these expectations we hope to provide a foundation for achieving consistency across
languages for NSQ users.

For details on the low-level TCP protocol, see the [protocol doc](protocol.md).

### Overview

An NSQ client can either produce or consume messages.  For consumers, the focal points are:

 1. Configuration
 2. Discovery (optional)
 3. Connection
 4. Data Flow / Heartbeats
 5. Message Handling
 6. RDY State Management
 7. Backoff
 8. Reconnection

### Configuration

A client subscribes to a `topic` on a `channel` over a TCP connection to `nsqd` instance(s). You can
only subscribe to one topic per connection so multiple topic consumption needs to be structured
accordingly.

Using `nsqlookupd` is optional so client libraries should support a configuration where a client
connects *directly* to one or more `nsqd` instances or where it is configured to poll one or more
`nsqlookupd` instances.  For more detail see [Discovery](#discovery).

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

When a client is configured to poll `nsqlookupd` the polling interval should be configurable.
Additionally, because typical deployments of NSQ are in distributed environments with many producers
and consumers, the client library should automatically add jitter based on a random % of the
configured value. This will help avoid a thundering herd of connections.

### Discovery

An important component of NSQ is `nsqlookupd` which provides a discovery service for consumers to
locate producers of a given topic at runtime.