import {createSearchIndex, dropSearchIndex} from "jstests/libs/search.js";

const collName = "docs";
const coll = db.getCollection(collName);
coll.drop();

coll.insertMany([
    {_id: 0, title: "Star Trek", genres: ["sci-fi", "adventure"], awards: 2, release: ISODate("1979-12-07T00:00:00Z")},
    {_id: 1, title: "Star Wars", genres: ["sci-fi", "action"], awards: 3, release: ISODate("1977-05-25T00:00:00Z")},
    {_id: 2, title: "Star Chronicles", genres: ["drama"], awards: 5, release: ISODate("2005-09-15T00:00:00Z")},
    {_id: 3, title: "Star Quest", genres: ["sci-fi", "fantasy"], awards: 17, release: ISODate("2010-03-12T00:00:00Z")},
    {_id: 4, title: "Star Legends", genres: ["fantasy"], awards: 19, release: ISODate("2010-11-19T00:00:00Z")}
  ]);

const indexSpec = {
    name: "default",
    definition: {
        name: "default",
        mappings: {
            dynamic: true,
            fields: {
                genres: { type: "stringFacet" },
                awards: { type: "numberFacet" },
                release: { type: "dateFacet" }
            }
        }
    }
};

createSearchIndex(coll, indexSpec);

// string facet
const stringFacetResult = assert.commandWorked(db.runCommand({
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
            },
            "count": {
              "type": "total"
            }
        }
    },
    { $project: { _id: 1, title: 1, meta: "$$SEARCH_META"} },
    { $sort: {_id: 1 } },
    { $limit: 1}],
    cursor: {}
}));

assert.eq(stringFacetResult.cursor.firstBatch, [
    {
      _id: 0,
      title: "Star Trek",
      meta: {
        count: { total: NumberLong(5) },
        facet: {
          genresFacet: {
            buckets: [
              { _id: "sci-fi", count: NumberLong(3) },
              { _id: "fantasy", count: NumberLong(2) },
              { _id: "action", count: NumberLong(1) },
              { _id: "adventure", count: NumberLong(1) },
              { _id: "drama", count: NumberLong(1) }
            ]
          }
        }
      }
    }
  ]);
  
  // number facet
  const numberFacetResult = assert.commandWorked(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
      {
        $pluginSearch: {
          facet: {
            operator: {
              text: {
                query: "Star",
                path: "title"
              }
            },
            facets: {
              awardsFacet: {
                type: "number",
                path: "awards",
                boundaries: [0, 10, 20]
              }
            }
          },
          count: {
            type: "total"
          }
        }
      },
      { $project: { _id: 1, title: 1, meta: "$$SEARCH_META" } },
      { $sort: { _id: 1 } },
      { $limit: 1 }
    ],
    cursor: {}
  }));

  assert.eq(numberFacetResult.cursor.firstBatch, [
    {
      _id: 0,
      title: "Star Trek",
      meta: {
        count: { total: NumberLong(5) },
        facet: {
          awardsFacet: {
            buckets: [
              { _id: 0, count: NumberLong(3) },
              { _id: 10, count: NumberLong(2) }
            ]
          }
        }
      }
    }
  ]);
  
  // date facet
  const dateFacetResult = assert.commandWorked(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
      {
        $pluginSearch: {
          facet: {
            operator: {
              text: {
                query: "Star",
                path: "title"
              }
            },
            facets: {
              releaseFacet: {
                type: "date",
                path: "release",
                boundaries: [
                  ISODate("1900-12-01"),
                  ISODate("2000-12-01"),
                  ISODate("3000-12-01")
                ]
              }
            }
          },
          count: {
            type: "total"
          }
        }
      },
      { $project: { _id: 1, title: 1, meta: "$$SEARCH_META" } },
      { $sort: { _id: 1 } },
      { $limit: 1 }
    ],
    cursor: {}
  }));

  assert.eq(dateFacetResult.cursor.firstBatch, [
    {
      _id: 0,
      title: "Star Trek",
      meta: {
        count: { total: NumberLong(5) },
        facet: {
          releaseFacet: {
            buckets: [
              {
                _id: ISODate("1900-12-01T00:00:00Z"),
                count: NumberLong(2)
              },
              {
                _id: ISODate("2000-12-01T00:00:00Z"),
                count: NumberLong(3)
              }
            ]
          }
        }
      }
    }
  ]);
  
dropSearchIndex(coll, {name: "default"});
