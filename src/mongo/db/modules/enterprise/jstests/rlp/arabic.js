(function () {
    'use strict';

    load("fts_lib.js");

    db.ara.drop();

    // http://en.wikipedia.org/wiki/Arabic_verbs
    db.ara.insert({ _id: "past_writes", t1: "كَتَبْتُ" });
    db.ara.insert({ _id: "present_writes", t1: "أَكْتُبُ" });
    db.ara.insert({ _id: "future_writes", t1: "سَأَكْتُبُ" });

    db.ara.ensureIndex({ t1: "text" }, { default_language: "arabic" });

    // Positive Term Match
    assert.eq(["past_writes", "future_writes"], queryIds(db.ara, "كتب"));

    // Phrase Match
    assert.eq(["past_writes"], queryIds(db.ara, 'كتب "كَتَبْتُ"'));

    // Negative Phrase Match
    assert.eq(["future_writes"], queryIds(db.ara, 'كتب -"كَتَبْتُ"'));
}) ();