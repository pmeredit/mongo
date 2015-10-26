(function () {
    'use strict';

    load("src/mongo/db/modules/enterprise/jstests/rlp/fts_lib.js");

    db.zht.drop();

    // Beijing University Biology Department
    db.zht.insert({ _id: 1, t1: "北京大學生物系" });

    db.zht.ensureIndex({ t1: "text" }, { default_language: "traditional chinese" });

    // Positive Term Match
    assert.eq([], queryIds(db.zht, "北京大学"));
    assert.eq([1], queryIds(db.zht, "北京大學"));
    assert.eq([], queryIds(db.zht, "学生"));

    // Negative Term Match
    assert.eq([1], queryIds(db.zht, "北京大學 -学生"));
    assert.eq([], queryIds(db.zht, "北京大學 -生物系"));
    assert.eq([1], queryIds(db.zht, "北京大學 -生物"));

    // Phrase Match
    assert.eq([1], queryIds(db.zht, '北京大學 "學生"'));

    // Negative Phrase Match
    assert.eq([], queryIds(db.zht, '北京大學 -"學生"'));
})();
