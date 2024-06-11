/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {getDefaultSp, test} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';

const inputColl = db.getSiblingDB('test')['input'];
inputColl.drop();
inputColl.insertOne({_id: 1});
assert.eq(inputColl.count(), 1);

// to make sure the timestamp version of a date starts on a new second so there's no ambiguity
sleep(1000);

// throwaway write to update the clusterTime
db.getSiblingDB('test')['throwaway'].insertOne({_id: 0});
db.getSiblingDB('test')['throwaway'].drop();

const startTime = db.hello().$clusterTime.clusterTime;
const startDate = new Date(startTime.getTime() * 1000);
inputColl.insertOne({_id: 2});

assert.eq(startTime.getTime(), Timestamp(startDate.getTime() / 1000, 0).getTime());
assert.eq(inputColl.count(), 2);

const sp = getDefaultSp();
const spTimestamp = 'timestamp';
const spDate = 'date';

const outputCollTimestamp = db.getSiblingDB('test')['timestamp'];
const outputCollDate = db.getSiblingDB('test')['date'];
outputCollTimestamp.drop();
outputCollDate.drop();

assert.eq(outputCollTimestamp.count(), 0);
assert.eq(outputCollDate.count(), 0);

sp.createStreamProcessor(spTimestamp, [
    {
        $source: {
            connectionName: test.atlasConnection,
            db: test.dbName,
            coll: 'input',
            config: {
                startAtOperationTime: startTime,
            }
        }
    },
    {$merge: {into: {connectionName: test.atlasConnection, db: test.dbName, coll: 'timestamp'}}}
]);

sp.createStreamProcessor(spDate, [
    {
        $source: {
            connectionName: test.atlasConnection,
            db: test.dbName,
            coll: 'input',
            config: {
                startAtOperationTime: startDate,
            }
        }
    },
    {$merge: {into: {connectionName: test.atlasConnection, db: test.dbName, coll: 'date'}}}
]);

sp[spTimestamp].start();
assert.soon(() => { return outputCollTimestamp.count() > 0; });
sp[spTimestamp].stop();

sp[spDate].start();
assert.soon(() => { return outputCollDate.count() > 0; });
sp[spDate].stop();

assert.eq(outputCollTimestamp.count(), 1);
assert.eq(outputCollTimestamp.findOne().fullDocument._id, 2);

assert.eq(outputCollDate.count(), 1);
assert.eq(outputCollDate.findOne().fullDocument._id, 2);