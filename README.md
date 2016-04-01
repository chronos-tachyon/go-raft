# go-raft
An implementation of the [Raft distributed consensus protocol](https://raft.github.io)

[![godoc](https://chronos-tachyon.net/img/godoc-badge.svg)](https://godoc.org/github.com/chronos-tachyon/go-raft)

Status: work in progress

## What is it?
Raft, if you're not familiar, is a consensus protocol for distributed systems.
It works by (a) electing a leader, and then (b) recording and distributing a
log of state mutations.
