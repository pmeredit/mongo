import {createSearchIndex, dropSearchIndex} from "jstests/libs/search.js";

const collName = "docs";
const coll = db.getCollection(collName);
coll.drop();

coll.insertMany([
    {_id: 0, title: "Star Trek", genres: ["sci-fi", "adventure"]},
    {_id: 1, title: "Star Wars", genres: ["sci-fi", "action"]},
    {_id: 2, title: "Star Chronicles", genres: ["drama"]},
    {_id: 3, title: "Star Quest", genres: ["sci-fi", "fantasy"]},
    {_id: 4, title: "Star Legends", genres: ["fantasy"]}
]);

const indexSpec = {
    name: "default",
    definition:
        {"name": "default", "mappings": {"dynamic": true, "fields": {"genres": {"type": "stringFacet"}}}, "storedSource": {"exclude": ["title"]}}
};

createSearchIndex(coll, indexSpec);

const res = assert.commandWorked(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [{ 
        "$pluginSearch": {
            "facet": {
                "operator": {
                    "text": {
                        "query": "Star",
                        "path": "title"
                    }
                },
                "facets": {
                    "genresFacet": {
                    "type": "string",
                    "path": "genres"
                    }
                }
            }
        }
    }],
    cursor: {batchSize: 0}
}));

assert.eq(res.cursors.length, 2);

const primaryCursor = res.cursors[0].cursor;

let primaryCursorGetMore =
    db.runCommand({getMore: primaryCursor.id, collection: coll.getName()});

assert.eq(primaryCursorGetMore.cursor.nextBatch.sort((a, b) => a._id - b._id), [
    {_id:0, title:"Star Trek", genres:["sci-fi","adventure"]},
    {_id:1, title:"Star Wars", genres:["sci-fi","action"]},
    {_id:2, title:"Star Chronicles", genres:["drama"]},
    {_id:3, title:"Star Quest", genres:["sci-fi","fantasy"]},
    {_id:4, title:"Star Legends", genres:["fantasy"]}
  ]);

const secondaryCursor = res.cursors[1].cursor;
let secondaryCursorGetMore =
    db.runCommand({getMore: secondaryCursor.id, collection: coll.getName()});

assert.eq(secondaryCursorGetMore.cursor.nextBatch, 
    [{"type":"count","count": NumberLong(5)},
    {"type":"facet","tag":"genresFacet","bucket":"action","count": NumberLong(1)},
    {"type":"facet","tag":"genresFacet","bucket":"adventure","count": NumberLong(1)},
    {"type":"facet","tag":"genresFacet","bucket":"drama","count": NumberLong(1)},
    {"type":"facet","tag":"genresFacet","bucket":"fantasy","count": NumberLong(2)},
    {"type":"facet","tag":"genresFacet","bucket":"sci-fi","count": NumberLong(3)}]
);

dropSearchIndex(coll, {name: "default"});
