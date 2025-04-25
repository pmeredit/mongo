const coll = db.add_fields_dotted_paths;
coll.drop();

const input = {
    "c": "C!"
}

              assert.eq(coll.aggregate([{$echoOxide: input}]).toArray(), [input]);

assert.eq(coll.aggregate([{$echoWithSomeCrabs: {document: input, numCrabs: 0}}]).toArray(),
          [input]);
assert.eq(coll.aggregate([{$echoWithSomeCrabs: {document: input, numCrabs: 3}}]).toArray(), [{
              "c": "C!",
              "someCrabs": "🦀🦀🦀",
          }]);
assert.eq(coll.aggregate([{$helloWorldWithFiveCrabs: {}}]).toArray(),
          [{"hello": "world", "someCrabs": "🦀🦀🦀🦀🦀"}]);