/*
Package spooler is the Go client for Spooler, a managed message queue.

[Client.Send] sends a message, [Client.Recv] receives and leases it.
[Client.Renew] renews the lease.
[Client.Ack], [Client.Nack], [Client.Release], [Client.Fail] and
[Client.Discard] settle the message.

More info is on https://docs.spooler.sh/delivery.
*/
package spooler
