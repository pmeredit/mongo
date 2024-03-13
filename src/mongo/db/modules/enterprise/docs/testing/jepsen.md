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

-   Overrides and extends the time limit for cycle searches in elle
-   Updates the MongoDB driver
-   Supports the hello command, instead of the now-removed `isMaster` command
-   Does not download or install MongoDB. Jepsen expects the binaries to be
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

## Jepsen Tests in Evergreen

### Plain Jepsen Test

These are the basic Jepsen tests targeting certain operations, including
three types:

-   _set_: The test will do a lot of write, followed by a final read.
-   _register_: The test will do a lot of compare-and-set against a single
    document.
-   _read-concern-majority_: The test is to test that majority committed
    write will never rollback. It is achieved by continually inserting unique
    documents using many writer threads, while a single thread periodically
    reads from the collection.

At the moment, we have the following test cases making use of the above
three types:

-   Read-concern-majority
-   Read-concern-majority w: 1
-   Register-findAndModify
-   Register-LinearizableRead
-   Set-LinearizableRead

You can take [this evergreen task](https://github.com/10gen/mongo/blob/v7.1/etc/evergreen_yml_components/definitions.yml#L3848-L3859)
as a startpoint to look at the [setup scripts](https://github.com/10gen/mongo/tree/v7.1/evergreen/do_jepsen_setup)
for these Jepsen tests.

### Jepsen Docker Test

This test is running on a 9-node docker setup, including a 3-node config shard
and two 3-node data shards. Currently, there is only one test workload
list-append, which has multiple clients keep running transactions with a
combination of findOne and updateOne.

To run this test locally, you can refer to [this StackOverflow question](https://mongodb.stackenterprise.co/questions/889).

### Jepsen Test x Config Fuzzer

[Config fuzzer](https://github.com/10gen/mongo/blob/v7.1/buildscripts/resmokelib/generate_fuzz_config/__init__.py#L16)
is a script to randomly generate the configuration file for mongod and mongos.
[The Jepsen config fuzzer test](https://github.com/10gen/mongo/blob/v7.1/etc/evergreen_yml_components/definitions.yml#L3935)
is a combination of config fuzzer and all the Jepsen tests mentioned above to
add more randomness for Jepsen tests.

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
when the test succeeds, `Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻` when the tests ran
and found a consistency error, and will emit a non-zero error code without
either of those strings when an internal failure occurred.

When triaging BFs, tests emitting `Analysis invalid` should be added to
Replication's backlog for triage, while other failures should be added to
the Correctness (SDP) Backlog

Note that new-style Jepsen tests feature a repeat flag. If multiple test are run,
you will see multiple instances of `Everything looks good!` or `Analysis
invalid`, based on the number of repeated tests run. Jepsen does not aggregate
repeated tests into a single pass or fail. See `buildscripts/jepsen_report.py`
for the aggregation script used to parse `list-append`s output.

Note that a single task can have different types of failures, some of which
need to be referred to SDP, and others to Replication. Tests which are listed
as "Crashed" should be referred to SDP, while all others should be referred
to Replication.

## Typical Issues

-   In this [BF](https://jira.mongodb.org/browse/BF-29752), Jepsen failed due to use of unexpected consistency model and the analysis is helpful for identifying Jepsen issues.
