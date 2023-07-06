export const exprDocs = [
    {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: [0, 0]},
    {_id: 1, ssn: "456", name: "B", manager: "C", age: NumberLong(35), location: [0, 1]},
    {_id: 2, ssn: "789", name: "C", manager: "D", age: NumberLong(45), location: [0, 2]},
    {_id: 3, ssn: "123", name: "D", manager: "A", age: NumberLong(55), location: [0, 3]},
];

export const exprTestData = {
    matchFilters: [
        // Simple tests for single encrypted field comparisons using $eq, $ne, and $in.
        {filter: {$expr: {$eq: ['$ssn', "123"]}}, expected: [exprDocs[0], exprDocs[3]]},
        {filter: {$expr: {$ne: ['$ssn', "123"]}}, expected: [exprDocs[1], exprDocs[2]]},
        {filter: {$expr: {$in: ['$ssn', ["123"]]}}, expected: [exprDocs[0], exprDocs[3]]},
        {
            filter: {$expr: {$in: ['$ssn', ["123", "456"]]}},
            expected: [exprDocs[0], exprDocs[1], exprDocs[3]]
        },

        // Test the reverse order of $eq arguments. Note that the corresponding queries for $in are
        // not permitted within QA, e.g. {$expr: {$in: ["123", "$ssn"]}}.
        {filter: {$expr: {$eq: ["123", "$ssn"]}}, expected: [exprDocs[0], exprDocs[3]]},

        // Similar to the above, but only querying non-encrypted fields.
        {filter: {$expr: {$eq: ['$_id', 0]}}, expected: [exprDocs[0]]},
        {filter: {$expr: {$eq: ['$name', "B"]}}, expected: [exprDocs[1]]},
        {filter: {$expr: {$in: ['$name', ["C", "D"]]}}, expected: [exprDocs[2], exprDocs[3]]},
        {
            filter: {$expr: {$eq: ['$name', "$name"]}},
            expected: [exprDocs[0], exprDocs[1], exprDocs[2], exprDocs[3]]
        },
        {
            filter: {$expr: {$and: [{$in: ['$name', ["C", "D"]]}, {$eq: ['$_id', 2]}]}},
            expected: [exprDocs[2]]
        },

        // Similar to the above, but querying with (to-be-encrypted) constants which do not exist in
        // the collection.
        {filter: {$expr: {$eq: ['$ssn', "999"]}}, expected: []},
        {
            filter: {$expr: {$ne: ['$ssn', "999"]}},
            expected: [exprDocs[0], exprDocs[1], exprDocs[2], exprDocs[3]]
        },
        {filter: {$expr: {$in: ['$ssn', ["123", "999"]]}}, expected: [exprDocs[0], exprDocs[3]]},
        {filter: {$expr: {$in: ['$ssn', ["999", "111"]]}}, expected: []},
        {filter: {$expr: {$in: ['$ssn', []]}}, expected: []},

        // Test more complicated $expr shapes while querying multiple encrypted values.
        {
            filter:
                {$and: [{$expr: {$eq: ['$ssn', "123"]}}, {$expr: {$eq: ['$age', NumberLong(55)]}}]},
            expected: [exprDocs[3]]
        },
        {
            filter: {
                $expr: {
                    $and: [
                        {$in: ['$ssn', ["123", "456"]]},
                        {$in: ['$age', [NumberLong(55), NumberLong(35), NumberLong(60)]]}
                    ]
                }
            },
            expected: [exprDocs[1], exprDocs[3]]
        },
        {
            filter: {
                $expr: {
                    $or: [{$eq: ['$ssn', "123"]}, {$in: ['$age', [NumberLong(35), NumberLong(60)]]}]
                }
            },
            expected: [exprDocs[0], exprDocs[1], exprDocs[3]]
        },
        {
            filter: {$and: [{$expr: {$eq: ['$ssn', "123"]}}, {age: {$eq: NumberLong(55)}}]},
            expected: [exprDocs[3]]
        },

        // Test queries including comparisons to both encrypted and unencrypted fields.
        {
            filter: {$and: [{$expr: {$eq: ['$ssn', "123"]}}, {$expr: {$eq: ['$_id', 0]}}]},
            expected: [exprDocs[0]]
        },
        {
            filter: {$expr: {$and: [{$eq: ['$ssn', "123"]}, {$eq: ['$_id', 0]}]}},
            expected: [exprDocs[0]]
        },
        {
            filter: {$expr: {$and: [{$in: ['$ssn', ["123", "456"]]}, {$eq: ["$name", "B"]}]}},
            expected: [exprDocs[1]]
        }
    ],
    docs: exprDocs
};
