(function() {
    'use strict';

    load("src/mongo/db/modules/enterprise/jstests/rlp/fts_lib.js");

    db.zhs.drop();

    // Beijing University Biology Department
    db.zhs.insert({_id: 1, t1: "北京大学生物系"});

    db.zhs.ensureIndex({t1: "text"}, {default_language: "simplified chinese"});

    assert.eq(1, db.zhs.find({"$text": {"$search": "北京大学", "$language": "zhs"}}).itcount());

    // Positive Term Match
    assert.eq([1], queryIds(db.zhs, "北京大学"));
    assert.eq([], queryIds(db.zhs, "北京大學"));
    assert.eq([], queryIds(db.zhs, "学生"));

    // Negative Term Match
    assert.eq([1], queryIds(db.zhs, "北京大学 -学生"));
    assert.eq([], queryIds(db.zhs, "北京大学 -生物系"));
    assert.eq([1], queryIds(db.zhs, "北京大学 -生物"));

    // Phrase Match
    assert.eq([1], queryIds(db.zhs, '北京大学 "学生"'));

    // Negative Phrase Match
    assert.eq([], queryIds(db.zhs, '北京大学 -"学生"'));
})();
