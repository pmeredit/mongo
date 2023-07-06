export const aggDocs = [
    {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: [0, 0]},
    {_id: 1, ssn: "456", name: "B", manager: "C", age: NumberLong(35), location: [0, 1]},
    {_id: 2, ssn: "789", name: "C", manager: "D", age: NumberLong(45), location: [0, 2]},
    {_id: 3, ssn: "123", name: "D", manager: "A", age: NumberLong(55), location: [0, 3]},
];

export const fleAggTestData = {
    schema: {
        encryptedFields: {
            fields: [
                {path: "ssn", bsonType: "string", queries: {queryType: "equality"}},
                {path: "age", bsonType: "long", queries: {queryType: "equality"}}
            ]
        },
    },
    docs: aggDocs,
    tests: [
        //
        // Tests that check that single-stage aggregations which reference encrypted fields
        // return the correct results.
        //

        // $match with a filter on an encrypted field ($eq).
        {pipeline: [{$match: {ssn: "123"}}], expected: [aggDocs[0], aggDocs[3]]},
        // $match with a compound filter on an encrypted field ($eq) and a non-encrypted field.
        {pipeline: [{$match: {ssn: "123", _id: 0}}], expected: [aggDocs[0]]},
        // $geoNear with a filter on an encrypted field ($in).
        {
            pipeline: [{
                $geoNear: {
                    near: {type: "Point", coordinates: [0, 0]},
                    distanceField: "distance",
                    key: "location",
                    query: {ssn: {$in: ["789", "456"]}}
                }
            }],
            expected: [aggDocs[1], aggDocs[2]]
        },
        // $geoNear with a compound filter on an encrypted field ($ne) and a non-encrypted
        // field.
        {
            pipeline: [{
                $geoNear: {
                    near: {type: "Point", coordinates: [0, 0]},
                    distanceField: "distance",
                    key: "location",
                    query: {ssn: {$ne: "123"}, _id: 1}
                }
            }],
            expected: [aggDocs[1]]
        },

        //
        // Tests that check that if there are multiple stages in a pipeline, each stage
        // referencing an encrypted field is properly encrypted, and stages which do not
        // reference encrypted fields are not modified.
        //

        // $match on a non-encrypted field.
        {pipeline: [{$match: {name: "A"}}], expected: [aggDocs[0]]},
        // $match on an encrypted field followed by a group with no references to encrypted
        // fields.
        {
            pipeline: [
                {$match: {ssn: {$in: ["123", "456"]}}},
                {$group: {_id: "$manager", numReports: {$count: {}}}}
            ],
            expected:
                [{_id: "B", numReports: 1}, {_id: "C", numReports: 1}, {_id: "A", numReports: 1}]
        },
        // $project including an encrypted field.
        {
            pipeline: [{$project: {_id: 0, ssn: 1}}],
            expected: [{ssn: "123"}, {ssn: "456"}, {ssn: "789"}, {ssn: "123"}]
        },

        //
        // Tests that check that when there are multiple encrypted fields referenced in the
        // pipeline, each field is handled appropriately.
        //

        // $match on two encrypted fields ($eq).
        {pipeline: [{$match: {ssn: "123", age: NumberLong(55)}}], expected: [aggDocs[3]]},
        // $match on two encrypted fields ($or, $and, $in, $ne, $eq).
        {
            pipeline: [{
                $match: {
                    $or: [
                        {$and: [{ssn: "123"}, {age: NumberLong(55)}]},
                        {$and: [{ssn: {$in: ["456", "789"]}}, {age: {$ne: NumberLong(25)}}]}
                    ]
                }
            }],
            expected: [aggDocs[3], aggDocs[2], aggDocs[1]]
        },
        // $geoNear which filters on two encrypted fields ($in, $eq).
        {
            pipeline: [{
                $geoNear: {
                    near: {type: "Point", coordinates: [0, 0]},
                    distanceField: "distance",
                    key: "location",
                    query: {ssn: {$in: ["789", "456"]}, age: NumberLong(35)}
                }
            }],
            expected: [aggDocs[1]]
        },
    ]
};
