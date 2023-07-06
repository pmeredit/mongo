/**
 * Check that query analysis prevents view creation.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");

const schema = {
    encryptionInformation: {
        type: 1,
        schema: {
            "test.myview": {fields: []},
        }
    }
};

assert.commandFailedWithCode(
    testDB.runCommand(Object.assign(
        {create: "myview", pipeline: [{$match: {ssn: "ABC123"}}], viewOn: "coll"}, schema)),
    7501300);
assert.commandFailedWithCode(
    testDB.runCommand(Object.assign(
        {create: "myview", pipeline: [{$match: {name: "Bert"}}], viewOn: "coll"}, schema)),
    7501300);

mongocryptd.stop();