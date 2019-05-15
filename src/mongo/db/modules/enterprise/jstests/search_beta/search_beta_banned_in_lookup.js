/**
 * Test that $searchBeta is banned in a $lookup subpipeline.
 */

(function() {
    "use strict";

    function checkLookupBanned(conn) {
        const db = conn.getDB("test");
        const coll = db["internal_search_beta_banned_in_lookup"];
        const foreignColl = db[coll.getName() + "_foreign"];

        assert.commandWorked(foreignColl.insert({a: 1}));
        assert.commandWorked(coll.insert({a: 1}));

        {
            const pipeline = [
                {
                  $lookup: {
                      let : {},
                      pipeline: [
                          {$searchBeta: {}},
                      ],
                      from: foreignColl.getName(),
                      as: "c",
                  }
                },
            ];
            const err = assert.throws(() => coll.aggregate(pipeline));
            assert.commandFailedWithCode(err, 51047);
        }

        // $searchBeta within a $lookup within a $lookup.
        {
            const nestedPipeline = [
                {
                  $lookup: {
                      pipeline: [{
                          $lookup: {
                              pipeline: [
                                  {$searchBeta: {}},
                              ],
                              from: foreignColl.getName(),
                              as: "c",
                          }
                      }],
                      from: foreignColl.getName(),
                      as: "c",
                  }
                },
            ];
            const err = assert.throws(() => coll.aggregate(nestedPipeline));
            assert.commandFailedWithCode(err, 51047);
        }
    }

    {
        const conn = MongoRunner.runMongod();
        checkLookupBanned(conn);
        MongoRunner.stopMongod(conn);
    }

    {
        const st = new ShardingTest({shards: 2});
        checkLookupBanned(st.s);
        st.stop();
    }
})();
