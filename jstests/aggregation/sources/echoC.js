const coll = db.add_fields_dotted_paths;
coll.drop();

const input = { "c": "C!" }

assert.eq(coll.aggregate([{"$echoC": input}]).toArray(), [input])