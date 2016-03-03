(function() {
    'use strict';

    load("src/mongo/db/modules/enterprise/jstests/rlp/fts_lib.js");

    db.pes.drop();

    // http://en.wikipedia.org/wiki/Persian_grammar
    // My dog is smaller than your cat.
    db.pes.insert({_id: 1, t1: "سگ من از گربه‌ی تو کوچک‌تر است"});

    db.pes.ensureIndex({t1: "text"}, {default_language: "iranian persian"});

    assert.eq(1, db.pes.find({"$text": {"$search": "گرب", "$language": "pes"}}).itcount());

    // Positive Term Match
    assert.eq([1], queryIds(db.pes, "گرب"));

    // Negative Term Match
    assert.eq([], queryIds(db.pes, "گرب -اس"));

    // Phrase Match
    assert.eq([1], queryIds(db.pes, 'گرب "کوچک‌تر است"'));

    // Negative Phrase Match
    // assert.eq([1], queryIds(db.pes, 'كتب -"كَتَبْتُ"'));

})();
