(function () {
    'use strict';

    load("src/mongo/db/modules/enterprise/jstests/rlp/fts_lib.js");

    db.prs.drop();

    // http://en.wikipedia.org/wiki/Dari_language
    // to speak - persian farsi/persian dari
    db.prs.insert({ _id: 1, t1: "حرف زدن/گپ زدن" });

    db.prs.ensureIndex({ t1: "text" }, { default_language: "dari" });

    // Positive Term Match
    assert.eq([1], queryIds(db.prs, "زد"));

    // Negative Term Match
    assert.eq([], queryIds(db.prs, "زد -حرف"));

    // Phrase Match
    assert.eq([1], queryIds(db.prs, 'زد "زدن/گپ"'));

    // Negative Phrase Match
    //assert.eq([1], queryIds(db.prs, 'كتب -"كَتَبْتُ"'));
})();
