# Jepsen Testing

In an effort to prevent regressions, we run the infamous Jepsen suite against
MongoDB inside of Evergreen.

## Jepsen Repositories

### jepsen-io/jepsen
Contains the jepsen core, but not the MongoDB tests. See the jepsen
subdirectory for the code. Note that the mongodb-rocks and mongodb-smartos
directories are not the tests we run

### jepsen-io/elle
The Jepsen transaction checker. Imported by jepsen core. Note that we use
Clojure's `with-redefs` to dynamically replace some parts of elle to avoid forking
this repository. See `10gen/jepsen-io-mongodb:src/jepsen/mongodb.clj`

### jepsen-io/mongodb
The actual MongoDB tests. Due to the total rewrite, this repository contains
both the 'old' and 'new' style tests.

The tests present prior to commit `a4b48558202f7f2a5175fca74fa2c784af6b03f7`
are referred to as 'old' style tests, while tests present including and after
this commit are referred to as 'new' style tests. 

Note that we do not run tests from this repository. These tests are configured
to download and install our publicly distributed Debian packages. See
immediately below for our Evergreen-equipped repositories.

### 10gen/jepsen-io-mongodb
Our internal fork of `jepsen-io/mongodb`, containing Evergreen specific
infrastructure and utility changes to enable running new-style Jepsen tests
against pre-release versions of MongoDB.

#### Branch: no-download-master

* Overrides and extends the time limit for cycle searches in elle
* Updates the MongoDB driver
* Supports the hello command, instead of the now-removed `isMaster` command
* Does not download or install MongoDB. Jepsen expects the binaries to be
copied to a directory in the `PATH`. This allows running Jepsen with Evergreen
builds

### 10gen/jepsen

#### Branch: jepsen-mongodb-master
Home of the "old-style" Jepsen tests, as we run them in Evergreen.

These tests were built with the assumption that each node of the database
would run on a different system with full control of the system clock (i.e.
Not Docker, LXC, OpenVZ, or a paravirtualization system. Bare metal or any fully
virtualized solution, including AWS EC2 or KVM, is acceptable)

At the time of original implementation, Evergreen did not have hosts.create, or
any other feature that would allow us to create real systems for each node
of the topology, so a solution was implemented using libfaketime. libfaketime
presents a fake system clock to each instance of mongod, even ones running on
the same system.

In this branch lives a fork of the jepsen repository that uses libfaketime
to manipulate the system clock, allowing all Jepsen nodes to run on a single
system.


### 10gen/libfaketime
"Old-style" Jepsen tests manipulate the system clock using libfaketime. This
repository contains our internal fork of libfaketime, with hooks used by
Jepsen. It is periodically necessary to merge upstream into our fork, such as
when changes to the Linux kernel's clock occur.

"New-style" Jepsen tests do not modify the system clock at time of writing.

The `for-jepsen` branch contains our changes, with the master branch remaining
unmodified from upstream.

## Common Procedures
### Driver Upgrades
Occassionally, BFs emerge from the Jepsen suites that originate from the
Java driver. In these scenarios, it is necessary to upgrade the Java Driver.

In the `no-download-master` branch of the `10gen/jepsen-io-mongodb` repository,

Open the project.clj file and note the line `[org.mongodb/mongodb-driver-sync "x.y.z"]`.

In the `jepsen-mongodb-master` branch of '10gen/jepsen':
Open the project.clj file and note the line `[org.mongodb/mongodb-driver "x.y.z"]`.

Consult the published Maven repository for the MongoDB driver and
update the driver version number here.

### Triaging Jepsen Test Results
A single iteration of Jepsen tests will emit `Everything looks good! ヽ(‘ー``)ノ`
when the test succeeds, `Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻`  when the tests ran
and found a consistency error, and will emit a non-zero error code without
either of those strings when an internal failure occurred.

When triaging BFs, tests emitting `Analysis invalid` should be added to
Replication's backlog for triage, while other failures should be added to
the Server Development Platform (SDP) Backlog

Note that new-style Jepsen tests feature a repeat flag. If multiple test are run,
you will see multiple instances of `Everything looks good!` or `Analysis
invalid`, based on the number of repeated tests run. Jepsen does not aggregate
repeated tests into a single pass or fail. See `buildscripts/jepsen_report.py`
for the aggregation script used to parse `list-append`s output.

Note that a single task can have different types of failures, some of which
need to be referred to SDP, and others to Replication. Tests which are listed
as "Crashed" should be referred to SDP, while all others should be referred
to Replication.
