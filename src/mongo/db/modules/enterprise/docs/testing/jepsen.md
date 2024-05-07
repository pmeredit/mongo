# Jepsen Testing

MongoDB uses [Jepsen](https://jepsen.io/) to find critical bugs that jeopardize our products’ correctness/consistency. The [owner](https://aphyr.com/about) of Jepsen uses their technology to find bugs and publishes their [findings](https://jepsen.io/analyses) for MongoDB ([4.2.6](https://jepsen.io/analyses/mongodb-4.2.6)) and a variety of other Database companies. This well-intentioned act can lead to poor/reactive PR for MongoDB, and thus we must proactively stay ahead of it by running the tests inside of Evergreen.

## At a Glance

There are two styles of Jepsen tests, each independent from each other and even use completely separate libraries.

| Style                  | jepsen                                                                                                                                                                                                                                                          | jepsen_docker                                                                                                                                                                                                                       |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Description            | “Plain” and “old version” Jepsen tests                                                                                                                                                                                                                          | “New” Jepsen tests that leverage Docker containers                                                                                                                                                                                  |
| Tasks run              | <ul><li>jepsen_read-concern-majority</li><li>jepsen_read-concern-majority_w1</li><li>jepsen_register_findAndModify</li><li>jepsen_register_linearizableRead</li><li>jepsen_set_linearizableRead</li></ul>(each repeated with their server configuration fuzzer) | <ul><li>jepsen_list-append</li></ul>(repeated with its server configuration fuzzer)                                                                                                                                                 |
| Setup script           | [evergreen/do_jepsen_setup/install_jepsen.sh](https://github.com/10gen/mongo/blob/master/evergreen/do_jepsen_setup/install_jepsen.sh)                                                                                                                           | [evergreen/jepsen_docker/setup.sh](https://github.com/10gen/mongo/blob/master/evergreen/jepsen_docker/setup.sh)                                                                                                                     |
| Clones from            | [10gen/jepsen --branch=jepsen-mongodb-master](https://github.com/10gen/jepsen)                                                                                                                                                                                  | [10gen/jepsen --branch=v0.2.0-evergreen-master](https://github.com/10gen/jepsen/releases/tag/v0.2.0-evergreen-master) and [10gen/jepsen-io-mongodb --branch=v0.2.0](https://github.com/10gen/jepsen-io-mongodb/releases/tag/v0.2.0) |
| Jepsen Version         | [0.1.8](https://github.com/10gen/jepsen/blob/cd30dfae9d1f3b3d7d966afce817c2d31a64b17d/project.clj#L9)                                                                                                                                                           | [0.3.5](https://github.com/10gen/jepsen-io-mongodb/blob/v0.2.0/project.clj#L7)                                                                                                                                                      |
| MongoDB Driver Version | [3.12.11](https://github.com/10gen/jepsen/blob/cd30dfae9d1f3b3d7d966afce817c2d31a64b17d/project.clj#L10)                                                                                                                                                        | [5.0.0](https://github.com/10gen/jepsen-io-mongodb/blob/v0.2.0/project.clj#L8)                                                                                                                                                      |
| JDK                    | [8](https://github.com/10gen/buildhost-configuration/blob/98f1964a98e03ff8b71466c444c4b6fd43aae1c2/roles/debian/tasks/ubuntu1804-x86_64.yml#L120)                                                                                                               | [17](https://github.com/10gen/jepsen/blob/a2d5e8767c661b2cf33d81cd61d18e6de2124783/docker/control/Dockerfile#L17)                                                                                                                   |
| Platforms              | [ubuntu1804-64](https://github.com/10gen/mongo/blob/dd61a8d04f3403fb4ad3b08477612e7a6ac57cfc/etc/evergreen_yml_components/variants/ubuntu/test_release.yml#L30)                                                                                                 | [ubuntu2204-64](https://github.com/10gen/mongo/blob/master/etc/evergreen_yml_components/variants/ubuntu/test_release.yml)                                                                                                           |

## Jepsen Repositories

### [jepsen-io/jepsen](https://github.com/jepsen-io/jepsen)

Contains the jepsen core, but not the MongoDB tests. See the jepsen
subdirectory for the code. Note that the `mongodb-rocks` and `mongodb-smartos`
directories are not the tests we run.

### [jepsen-io/elle](https://github.com/jepsen-io/elle)

The Jepsen transaction checker. Imported by jepsen core. Note that we use
Clojure's `with-redefs` to dynamically replace some parts of elle to avoid forking
this repository. See `10gen/jepsen-io-mongodb:src/jepsen/mongodb.clj`

### [jepsen-io/mongodb](https://github.com/jepsen-io/mongodb)

The actual MongoDB tests. Due to the total rewrite, this repository contains
both the 'old' and 'new' style tests.

The tests present prior to commit `a4b48558202f7f2a5175fca74fa2c784af6b03f7`
are referred to as 'old' style tests, while tests present including and after
this commit are referred to as 'new' style tests.

Note that we do not run tests from this repository. These tests are configured
to download and install our publicly distributed Debian packages. See
immediately below for our Evergreen-equipped repositories.

### [10gen/jepsen-io-mongodb](https://github.com/10gen/jepsen-io-mongodb)

Our internal fork of `jepsen-io/mongodb`, containing Evergreen specific
infrastructure and utility changes to enable running new-style Jepsen tests
against pre-release versions of MongoDB.

#### Branch: no-download-master

- Overrides and extends the time limit for cycle searches in elle
- Updates the MongoDB driver
- Supports the hello command, instead of the now-removed `isMaster` command
- Does not download or install MongoDB. Jepsen expects the binaries to be
  copied to a directory in the `PATH`. This allows running Jepsen with Evergreen
  builds

### [10gen/jepsen](https://github.com/10gen/jepsen)

#### Branch: `jepsen-mongodb-master` (default)

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

#### Branch: [`evergreen-master`](https://github.com/10gen/jepsen/tree/evergreen-master)

Our internal "fork" (no upstream linking) of [jepsen-io/jepsen](https://github.com/jepsen-io/jepsen), containing our customizations for Evergreen and Docker integrations.

### [10gen/libfaketime](https://github.com/10gen/libfaketime)

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

- _set_: The test will do a lot of write, followed by a final read.
- _register_: The test will do a lot of compare-and-set against a single
  document.
- _read-concern-majority_: The test is to test that majority committed
  write will never rollback. It is achieved by continually inserting unique
  documents using many writer threads, while a single thread periodically
  reads from the collection.

At the moment, we have the following test cases making use of the above
three types:

- Read-concern-majority
- Read-concern-majority w: 1
- Register-findAndModify
- Register-LinearizableRead
- Set-LinearizableRead

#### Running Locally

You can take [this evergreen task](https://github.com/10gen/mongo/blob/v7.1/etc/evergreen_yml_components/definitions.yml#L3848-L3859)
as a startpoint to look at the [setup scripts](https://github.com/10gen/mongo/tree/v7.1/evergreen/do_jepsen_setup)
for these Jepsen tests.

In summary, the steps resemble the following...

Ensure you have a Mongo binary to work with via [db_contrib_tool/setup_repro_env](https://github.com/10gen/db-contrib-tool/tree/main/src/db_contrib_tool/setup_repro_env):

```
cd ~/mongo
db-contrib-tool setup-repro-env

# should see some commit hash found here (latest from master)
ls ~/mongo/build/multiversion_bin/
```

```
# set a variable to this location, eg:
mongosrc=~/mongo/build/multiversion_bin/4f057a537dad84e2d2b7b389f0bb67b8f91abe79
```

Create a fresh working directory:

```
cd ~
rm -rf jepsen-repro-env
mkdir jepsen-repro-env
```

The [do jepsen setup](https://github.com/10gen/mongo/blob/9d9b2fd898674b3e066b6d3a4f6fc3f4102053f0/etc/evergreen_yml_components/definitions.yml#L1341) function is the following steps:

- [build_libfaketime.sh](https://github.com/10gen/mongo/blob/master/evergreen/do_jepsen_setup/build_libfaketime.sh)
- [install_jepsen.sh](https://github.com/10gen/mongo/blob/master/evergreen/do_jepsen_setup/install_jepsen.sh):
- [nodes.sh](https://github.com/10gen/mongo/blob/master/evergreen/do_jepsen_setup/nodes.sh)
- [move_binaries.sh](https://github.com/10gen/mongo/blob/master/evergreen/do_jepsen_setup/move_binaries.sh)
- [jepsen_test_run.sh](https://github.com/10gen/mongo/blob/master/evergreen/jepsen_test_run.sh)

```
## build_libfaketime.sh

cd ~/jepsen-repro-env
git clone --branch=for-jepsen --depth=1 git@github.com:10gen/libfaketime.git
cd libfaketime
make PREFIX=$(pwd)/build/ LIBDIRNAME='.' install


## install_jepsen.sh

cd ~/jepsen-repro-env
git clone --branch=jepsen-mongodb-master --depth=1 git@github.com:10gen/jepsen.git jepsen-mongodb
cd jepsen-mongodb
branch=$(git symbolic-ref --short HEAD)
commit=$(git show -s --pretty=format:"%h - %an, %ar: %s")
echo "Git branch: $branch, commit: $commit"

lein install


## nodes.sh

cd ~/jepsen-repro-env
python -c 'import socket; num_nodes = 5; print("\n".join(["%s:%d" % (socket.gethostname(), port) for port in range(20000, 20000 + num_nodes)]))' > nodes.txt


## move_binaries.sh

cp -rf $mongosrc/dist-test/bin/* .


## jepsen_test_run.sh

cd ~/jepsen-repro-env/jepsen-mongodb

workdir=/tmp/jepsen-repro-env/workdir
mkdir -p $workdir/tmp
_JAVA_OPTIONS=-Djava.io.tmpdir=$workdir/tmp
```

Run the `jepsen_read-concern-majority` test:

```
lein run test --test read-concern-majority \
  --mongodb-dir ../ \
  --working-dir ${workdir}/src/jepsen-workdir \
  --clock-skew faketime \
  --libfaketime-path ${workdir}/src/libfaketime/build/libfaketime.so.1 \
  --virtualization none \
  --nodes-file ../nodes.txt \
  --mongod-conf mongod_verbose.conf \
  --key-time-limit 15 \
  --protocol-version 1 \
  --storage-engine wiredTiger \
  --time-limit 120 \
  2>&1
```

### Jepsen Docker Test

This test is running on a 9-node docker setup, including a 3-node config shard
and two 3-node data shards. Roughly, the architecture is:

- Docker node N1: runs a `mongod`, `mongos`
- Docker node N2: runs a `mongod`, `mongos`
- ... (until N9)

Each Docker node runs a `mongod` and `mongos` within it, totalling 9 of each. The `mongod`s are divided into three replica sets: two shards and one config replica set.

Finally, there is a last container named `jepsen-control` that runs the Jepsen workload and issues commands to the other nodes. This node will contain all the Jepsen Clojure code that is run.

Currently, there is only one test workload,
"[list-append](https://github.com/10gen/jepsen-io-mongodb/blob/no-download-master/src/jepsen/mongodb/list_append.clj)", which has multiple clients keep running transactions with a
combination of `findOne` and `updateOne`.

#### Running Locally

Ensure you have a Mongo binary to work with via [db_contrib_tool/setup_repro_env](https://github.com/10gen/db-contrib-tool/tree/main/src/db_contrib_tool/setup_repro_env):

```
cd ~/mongo
db-contrib-tool setup-repro-env

# should see some commit hash found here (latest from master)
ls ~/mongo/build/multiversion_bin/
```

```
# set a variable to this location, eg:
mongosrc=~/mongo/build/multiversion_bin/4f057a537dad84e2d2b7b389f0bb67b8f91abe79
```

Create a fresh working directory:

```
cd ~
rm -rf jepsen-repro-env
mkdir jepsen-repro-env
cd jepsen-repro-env
```

The [run jepsen docker test](https://github.com/10gen/mongo/blob/b4d5c531a23bf3fb36d61a47c9eacc41f9b5d3da/etc/evergreen_yml_components/definitions.yml#L1389C4-L1389C26) function is the following steps:

- [evergreen/jepsen_docker/setup.sh](https://github.com/10gen/mongo/blob/master/evergreen/jepsen_docker/setup.sh)
- [docker-up.sh](https://github.com/10gen/mongo/blob/master/evergreen/jepsen_docker/docker-up.sh)
- [evergreen/jepsen_docker/list-append.sh](https://github.com/10gen/mongo/blob/master/evergreen/jepsen_docker/list-append.sh)

```
## setup.sh (confirm desired branches):

cd ~/jepsen-repro-env

git clone --branch=v0.1.0-evergreen-master git@github.com:10gen/jepsen.git jepsen

cp -rf $mongosrc/dist-test jepsen/docker/node

git clone --branch=v0.1.0 git@github.com:10gen/jepsen-io-mongodb.git jepsen/docker/control/mongodb

sudo docker container kill $(docker ps -q) || true


## docker-up.sh

cd ~/jepsen-repro-env/jepsen/docker
./bin/up -n 9 -d 2>&1


## list-append.sh

cd ~/jepsen-repro-env/jepsen/docker

sudo docker exec jepsen-control bash --login -c "\
  cd /jepsen/mongodb && \
  lein run test-all -w list-append \
  -n n1 -n n2 -n n3 -n n4 -n n5 -n n6 -n n7 -n n8 -n n9 \
  -r 1000 \
  --concurrency 3n \
  --time-limit 240 \
  --max-writes-per-key 128 \
  --read-concern majority \
  --write-concern majority \
  --txn-read-concern snapshot \
  --txn-write-concern majority \
  --nemesis-interval 1 \
  --nemesis partition \
  --test-count 1"
```

Descriptions of flags used above:

- `-r` This flag (r stands for rate) means that Jepsen writer client is to run at 1000 Hz, i.e. 1000 operations per second.
- `--concurrency` This specifies the number of writer clients. 3n means 3 writers per node. In this case we have 9 nodes, so we will have a total of 27 writer clients. Without the n it means that we want just 3 writers in total. So --concurrency 3 means just 3 writers in total.
- `--time-limit` This determines how long to run the workload for, in seconds. So in this case it will run for 240 seconds, i.e. 4 minutes.
- `--nemesis-interval` The time in seconds between Nemesis operations. Nemesis is Jepsen's fault introduction system. It introduces network partitions, etc.
- `--nemesis` Test partitions. If you don't want Nemesis to run at all use `--nemesis none`
- `--test-count` How many times Jepsen should run the test. In this case 30 times. Use a smaller number (like 1) to make it run just once.
- `--leave-db-running true` (not used above) Jepsen usually cleans up all logs / data files once it is done running within each Docker container. Setting this as such will disable that.

To stop Docker containers:

```
sudo docker container kill $(docker ps -q) || true
```

Finally, when you are sure you're not going to touch Docker for a few months, you can also run the following to clear up all temporary files, etc.:

```
docker system prune -a -f
```

##### Debugging Tips

To log in to a Docker container:

```
docker exec -it jepsen-n1 bash
```

You can inspect which processes are running here via `ps -aux | grep mongo`. You'll see both a `mongod` and a `mongos`, if you've already started running the Jepsen workload. You can connect to them via `mongo` shell: `mongo --port <27017 / 27018 ... >`

`mongod` and `mongos` use the params in `mongod.conf` and `mongos.conf` respectively. For example, `mongod.conf` tells `mongod` [where to log its output to](https://github.com/10gen/jepsen-io-mongodb/blob/8e6bea9b86bfcb45c58c32cf85998bd60228eaee/resources/mongod.conf#L17).

You can copy log files out of the containers like this (you run this when you are not logged in to the container):

```
docker cp jepsen-<YOUR NODE>:/var/log/mongodb/mongod.log <YOUR FILE>
```

When you make some change to Jepsen Clojure code, you need to stop (`docker container kill`) & rebuild (`./bin/up`) the Docker containers again, so that the Clojure code is loaded into the containers.

### Jepsen Test x Config Fuzzer

[Config fuzzer](https://github.com/10gen/mongo/blob/v7.1/buildscripts/resmokelib/generate_fuzz_config/__init__.py#L16)
is a script to randomly generate the configuration file for mongod and mongos.
[The Jepsen config fuzzer test](https://github.com/10gen/mongo/blob/v7.1/etc/evergreen_yml_components/definitions.yml#L3935)
is a combination of config fuzzer and all the Jepsen tests mentioned above to
add more randomness for Jepsen tests.

## Common Procedures

### Driver Upgrades

The specified versions can be found within the following:

- [project.clj](https://github.com/10gen/jepsen-io-mongodb/blob/no-download-master/project.clj) from `10gen/jepsen-io-mongodb`, of the form `[org.mongodb/mongodb-driver-sync "x.y.z"]`
  - Refer to supported versions from [this](https://mvnrepository.com/artifact/org.mongodb/mongodb-driver-sync) Maven repo
- [project.clj](https://github.com/10gen/jepsen/blob/jepsen-mongodb-master/project.clj) from `10gen/jepsen`, of the form `[org.mongodb/mongodb-driver "x.y.z"]`
  - Refer to supported versions from [this](https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver) Maven repo

### Triaging Jepsen Test Results

A single iteration of Jepsen tests will emit `Everything looks good! ヽ(‘ー``)ノ`
when the test succeeds, `Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻` when the tests ran
and found a consistency error, and will emit a non-zero error code without
either of those strings when an internal failure occurred.

When triaging BFs, tests emitting `Analysis invalid` should be added to
Replication's backlog for triage, while other failures should be added to
the Correctness Backlog.

Note that new-style Jepsen tests feature a repeat flag. If multiple test are run,
you will see multiple instances of `Everything looks good!` or `Analysis
invalid`, based on the number of repeated tests run. Jepsen does not aggregate
repeated tests into a single pass or fail. See `buildscripts/jepsen_report.py`
for the aggregation script used to parse `list-append`s output.

Note that a single task can have different types of failures, some of which
need to be referred to SDP, and others to Replication. Tests which are listed
as "Crashed" should be referred to SDP, while all others should be referred
to Replication.

## Jepsen Upgrades

### Update 10gen/jepsen

This is an examplar workflow that gets Jepsen's 0.3.5 source, and copies it into our 10gen/jepsen's [`evergreen-master`](https://github.com/10gen/jepsen/tree/evergreen-master) branch:

```
rm -rf jepsen035

git clone --branch=v0.3.5 git@github.com:jepsen-io/jepsen.git jepsen035

git clone git@github.com:10gen/jepsen.git 10gen-jepsen

cd 10gen-jepsen
git checkout evergreen-master
git checkout -B <<GITUSERNAME>>/jepsen-035

cd ..
cp -r 10gen-jepsen/.git keep
rm -rf 10gen-jepsen/
cp -r jepsen035 10gen-jepsen
rm -rf 10gen-jepsen/.git
cp -r keep 10gen-jepsen/.git
```

Push this one big commit as the Jepsen core upgrade, using the `evergreen-master` branch as your "upstream" origin.

Then layer on all of the MongoDB-specific changes to that, which may require some commit-auditing (this is the cost of maintaining an unlinked forked repo). The project's README should try to capture these succinctly.

### Iterate locally to test changes

See above for how to run the Jepsen Docker test locally, ensuring the `--branch` flags are updated correctly to point to your developer branches instead.

### Merge and Update

Once you can confirm that the upgrade can take place, merge your changes into 10gen/jepsen, tag it with an appropriate release, and update the branches in [evergreen/jepsen_docker/setup.sh](https://github.com/10gen/mongo/blob/master/evergreen/jepsen_docker/setup.sh).

## Typical Issues

- In this [BF](https://jira.mongodb.org/browse/BF-29752), Jepsen failed due to use of unexpected consistency model and the analysis is helpful for identifying Jepsen issues.
