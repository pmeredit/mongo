/**
 * Class for creating a sharded cluster where each (non config) data bearing node has an associated
 * mongotmock process.
 *
 * Public variables:
 * 'st' - Access the underlying ShardingTest
 */
class ShardingTestWithMongotMock {
    /**
     * Create a new ShardingTestWithMongotMock
     */
    constructor(shardingTestOptions) {
        this._shardingTestOptions = Object.assign({}, shardingTestOptions);

        this._nShards = Object.keys(shardingTestOptions.shards).length;
        this._mocksNeeded = 0;
        for (let i = 0; i < this._nShards; i++) {
            const msg =
                "ShardingTestWithMongotMock only supports 'shard' options passed to ShardingTest " +
                "of the form {shards: {rs0: <number of nodes>, rs1: <number of nodes>}}";
            assert.eq(typeof shardingTestOptions.shards["rs" + i].nodes, "number");

            this._mocksNeeded += shardingTestOptions.shards["rs" + i].nodes;
        }

        this._mongotMocks = [];
        // Internal mapping from host string to mongotmock connection.
        this._hostToMockMap = {};

        this.st = null;
    }

    /**
     *  Start some mongotmocks followed by a ShardingTest.
     */
    start() {
        for (let i = 0; i < this._mocksNeeded; ++i) {
            const path = MongoRunner.dataDir + "/mongotmock" + i;
            const mock = new MongotMock({dataDir: path});

            this._mongotMocks.push(mock);

            mock.start();
            this._hostToMockMap[mock.getConnection().host] = mock;
        }

        this.st = new ShardingTest(this._shardingTestOptions);

        // Restart each node in order to point it at its parnter mongotmock. Restarting the nodes
        // is necessary to work around SERVER-41281, and can be avoided once SERVER-41281 is fixed.

        // Now convert the sharding test parameters to a form where we can tell each node to use a
        // different mongotmock.
        let mongotMocksIdx = 0;
        for (let i = 0; i < this._nShards; ++i) {
            for (let j = 0; j < this._shardingTestOptions.shards["rs" + i].nodes; ++j) {
                this.st["rs" + i].restart(j, {
                    setParameter:
                        {mongotHost: this._mongotMocks[mongotMocksIdx++].getConnection().host}
                })
            }

            this.st["rs" + i].waitForMaster();
        }
    }

    /**
     *  Stop all of the nodes.
     */
    stop() {
        this.st.stop();

        for (let mock of this._mongotMocks) {
            mock.stop();
        }
    }

    /**
     * Given a member of the sharded cluster, find the mongot connected to it.
     */
    getMockConnectedToHost(conn) {
        const res = assert.commandWorked(conn.adminCommand({getParameter: 1, mongotHost: 1}));
        return this._hostToMockMap[res.mongotHost];
    }
}
