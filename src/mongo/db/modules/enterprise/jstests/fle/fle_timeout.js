// Validate fle shuts down after timeout.
//
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";

const mongocryptd = new MongoCryptD();

// Set idle timeout to 5 seconds
mongocryptd.start(5);

const conn = mongocryptd.getConnection();

// Validate that mongocryptd writes a valid json file as a pid file.
const pidObj = mongocryptd.readPidFile();
assert.eq(pidObj.port, mongocryptd.getPort());
assert.eq(pidObj.pid, mongocryptd.getPid());

// Close the only connection
conn.close();

// Wait for it to shutdown
sleep(10 * 1000);

const code = mongocryptd.stop();
print("Exit Code: " + code);
assert.eq(code, 12);  // ExitCode::kill
