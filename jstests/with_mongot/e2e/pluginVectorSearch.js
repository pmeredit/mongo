import {createSearchIndex, dropSearchIndex} from "jstests/libs/search.js";

const collName = "vectors";
const coll = db.getCollection(collName);
coll.drop();

assert.commandWorked(coll.insertMany([
    {
        _id: 0,
        title: "The Godfather",
        vector: [0.313, 0.841],
        plot:
            "The powerful Corleone crime family navigates betrayal, power struggles, and personal conflict, as Michael Corleone is slowly drawn into the family's dark world."
    },
    {
        _id: 1,
        title: "The Shawshank Redemption",
        vector: [0.512, 0.678],
        plot:
            "Two imprisoned men form a strong friendship over decades, finding hope and redemption through small acts of defiance in a harsh prison environment."
    },
    {
        _id: 2,
        title: "Indiana Jones and the Last Crusade",
        vector: [0.229, 0.905],
        plot:
            "Archaeologist Indiana Jones teams up with his father to search for the Holy Grail, facing Nazi adversaries and a series of dangerous challenges."
    },
    {
        _id: 3,
        title: "The Dark Knight",
        vector: [0.714, 0.632],
        plot:
            "Batman faces off against the anarchistic Joker, whose goal is to plunge Gotham City into chaos while challenging Batman's moral code."
    },
    {
        _id: 4,
        title: "Titanic",
        vector: [0.401, 0.789],
        plot:
            "A young couple from different social backgrounds fall in love aboard the ill-fated RMS Titanic, their romance doomed by tragedy as the ship sinks."
    },
    {
        _id: 5,
        title: "Pulp Fiction",
        vector: [0.655, 0.233],
        plot:
            "The lives of several characters intertwine in a series of eclectic, non-linear stories of crime, redemption, and black humor."
    },
    {
        _id: 6,
        title: "The Lion King",
        vector: [0.921, 0.478],
        plot:
            "A young lion prince named Simba overcomes personal loss and guilt to reclaim his rightful place as king of the Pride Lands."
    },
    {
        _id: 7,
        title: "The Matrix",
        vector: [0.318, 0.846],
        plot:
            "A hacker named Neo discovers that the world he lives in is a simulation controlled by machines, and he must join a group of rebels to fight for humanity's freedom."
    },
    {
        _id: 8,
        title: "Forrest Gump",
        vector: [0.537, 0.621],
        plot:
            "The extraordinary life of Forrest Gump unfolds as he unintentionally influences key historical events in America, all while searching for his childhood love, Jenny."
    },
    {
        _id: 9,
        title: "The Pursuit of Happyness",
        vector: [0.784, 0.356],
        plot:
            "A struggling salesman becomes homeless with his young son, and must find a way to build a better future for them, despite the odds against him."
    }
]));

const indexSpec = {
    name: "default",
    type: "vectorSearch",
    definition: {
        "fields":
            [{"type": "vector", "path": "vector", "numDimensions": 2, "similarity": "euclidean"}]
    }
};

createSearchIndex(coll, indexSpec);

let results = coll.aggregate([{
                      $pluginVectorSearch: {
                          path: "vector",
                          index: "default",
                          queryVector: [0.714, 0.632],
                          numCandidates: 10,
                          limit: 5
                      }
                  }])
                  .toArray();

printjson(results);
assert.eq(results.length, 5);
assert(results.every(doc => ["_id", "title", "vector", "plot"].every(key => key in doc) &&
                         Object.keys(doc).length === 4));

dropSearchIndex(coll, {name: "default"});
