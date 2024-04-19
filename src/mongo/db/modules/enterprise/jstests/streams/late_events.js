/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {listStreamProcessors} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

const STREAM_NAME = 'sp0';
const runLateDocumentsTest = ({connectionRegistry = [], $source, groupID, insert}) => {
    const connRegistry = [
        {name: 'db', type: 'atlas', options: {uri: `mongodb://${db.getMongo().host}`}},
        ...connectionRegistry,
    ];
    const dlq = {
        connectionName: 'db',
        db: 'test',
        coll: 'dlq',
    };
    const sp = new Streams(connRegistry);

    const source = {$source};
    const aggregation = {
        $tumblingWindow: {
            interval: {size: NumberInt(1), unit: 'second'},
            pipeline: [{
                $group: {
                    _id: groupID,
                    sum: {$sum: 1},
                }
            }]
        }
    };
    const sink = {$merge: {into: {connectionName: 'db', db: 'test', coll: 'sink'}}};

    db.sink.drop();
    db.dlq.drop();

    const stream = sp.createStreamProcessor(STREAM_NAME, [source, aggregation, sink]);
    stream.start({dlq});

    const head = [
        {timestamp: "2023-03-03T20:42:30.000Z", id: 1, value: 1},
    ];
    // These are late documents, but with allowedLateness moving to window operator
    // they will still be accepted because watermark computed for a batch once and processed at the
    // end.
    const lateDocuments = [
        {timestamp: "2023-03-03T20:42:28.000Z", id: 2, value: 1},
        {timestamp: "2023-03-03T20:42:27.000Z", id: 3, value: 1},
        {timestamp: "2023-03-03T20:42:26.000Z", id: 4, value: 1},
        {timestamp: "2023-03-03T20:42:25.000Z", id: 5, value: 1},
    ];
    // This document should close and emit the first window to the sink.
    const tail = [
        {timestamp: "2023-03-03T20:43:00.000Z", id: 6, value: 1},
    ];
    const documents = [...head, ...lateDocuments, ...tail];
    insert(head);
    insert(tail);
    insert(lateDocuments);

    // Wait for the first window to close and be published to the sink.
    assert.soon(() => db.sink.findOne({_id: 5}));
    assert.soon(() => { return db.dlq.count() == 0; });

    const stats = stream.stats();

    // All documents, even the ones that go into the DLQ should be accounted for
    // in the source input docs stat.
    assert.eq(stats['inputMessageCount'], documents.length);
    assert.eq(0, stats['operatorStats'][0]['dlqMessageCount']);
    assert.eq(0, stats['operatorStats'][0]['dlqMessageSize']);
    assert.soon(() => stream.stats()['outputMessageCount'] == documents.length - 1);

    // The following documents will be rejected because they arrive after windows
    // they belong to are closed.
    const lateDocuments2 = [
        {timestamp: "2023-03-03T20:42:28.000Z", id: 6, value: 1},
        {timestamp: "2023-03-03T20:42:27.000Z", id: 7, value: 1},
        {timestamp: "2023-03-03T20:42:26.000Z", id: 8, value: 1},
        {timestamp: "2023-03-03T20:42:25.000Z", id: 9, value: 1},
    ];
    insert(lateDocuments2);
    assert.soon(() => { return db.dlq.count() == lateDocuments2.length; });
    const stats2 = stream.stats();
    jsTestLog(stats2);
    assert.eq(stats2['inputMessageCount'], documents.length + lateDocuments2.length);
    assert.eq(stats2['outputMessageCount'], documents.length - 1);
    assert.eq(lateDocuments2.length, stats2['dlqMessageCount']);
    assert.gt(stats2['dlqMessageSize'],
              0);  // not checking for exact size as that could change with dlq message changes.
    stream.stop();
};

(function testSourceLateDocuments_Kafka() {
    runLateDocumentsTest({
        connectionRegistry: [{
            name: 'kafka',
            type: 'kafka',
            options: {bootstrapServers: 'localhost:9092', isTestKafka: true}
        }],
        $source: {
            connectionName: 'kafka',
            topic: 'topic',
            timeField: {$dateFromString: {'dateString': '$timestamp'}},
            testOnlyPartitionCount: NumberInt(1),
        },
        groupID: '$id',
        insert: (documents) => {
            assert.commandWorked(db.runCommand({
                streams_testOnlyInsert: '',
                name: STREAM_NAME,
                documents,
            }));
        },
    });
})();

(function testSourceLateDocuments_ChangeStream() {
    runLateDocumentsTest({
        $source: {
            connectionName: 'db',
            db: 'test',
            coll: 'source',
            timeField: {$dateFromString: {'dateString': '$fullDocument.timestamp'}},
        },
        groupID: '$fullDocument.id',
        insert: (documents) => db.source.insertMany(documents),
    });
})();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);