'use strict';

load("fts_lib.js");

// All have been translated from English by Google Translate
// "How old are you?"
var languages = {
    english: {
        document : "How old are you?",
        terms: ['old']
    },
    spanish: {
        document : "¿Cuántos años tienes tú?",
        terms: ['años']
    },
    czech: {
        document : "Jak jsi starý?",
        terms: ['jsi']
    },
    greek: {
        document : "Πόσο χρονών είσαι?",
        terms: ['χρονών']
    },
    french: {
        document : "Quel âge avez-vous?",
        terms: ['âge']
    },
    hungarian: {
        document : "Hány éves vagy ?",
        terms: ['éves']
    },
    italian: {
        document : "Quanti anni hai?",
        terms: ['anni']
    },
    dutch: {
        document : "Hoe oud ben je?",
        terms: ['oud']
    },
    polish: {
        document : "Ile masz lat?",
        terms: ['masz']
    },
    portuguese: {
        document : "Quantos anos você tem?",
        terms: ['anos']
    },
    russian: {
        document : "Сколько тебе лет?",
        terms: ['тебе']
    },
    japanese: {
        document : "あなたは何歳ですか？",
        terms: ['何歳']
    },
    korean: {
        document : "당신은 몇 살 입니까?",
        terms: ['당신은']
    },
    finnish: {
        document : "Kuinka vanha olet?",
        terms: ['vanha']
    },
    turkish: {
        document : "Kaç yaşındasınız?",
        terms: ['Kaç']
    },
    /* NO LICENSE
    albanian: {
        document : "Sa vjeç jeni?",
        terms: ['vjeç']
    },*/
    /* NO LICENSE
    bulgarian: {
        document : "На колко години си?",
        terms: ['колко']
    },*/
    /* NO LICENSE
    catalan: {
        document : "Quants anys tens ?",
        terms: ['anys']
    },
    /* NO LICENSE
    croatian: {
        document : "Koliko imaš godina?",
        terms: ['imaš']
    },*/
    /* NO LICENSE
    estonian: {
        document : "Kui vana sa oled?",
        terms: ['vana']
    },*/
    danish: {
        document : "Hvor gammel er du ?",
        terms: ['gammel']
    },
    hebrew: {
        document : "בן כמה אתה?",
        terms: ['כמה']
    },
    /* NO LICENSE
    indonesian: {
        document : "Berapa umurmu ?",
        terms: ['umurmu']
    },*/
    /* NO LICENSE
    latvian: {
        document : "Cik tev gadu ?",
        terms: ['tev']
    },*/
    /* NO LICENSE
    malay: {
        document : "Berapa usia anda?",
        terms: ['usia']
    },*/
    norwegian: {
        document : "Hvor gammel er du?",
        terms: ['gammel']
    },
    /*pushto: {
        document : "Kaç yaşındasınız?",
        terms: ['Kaç']
    },*/
    romanian: {
        document : "Cati ani ai?",
        terms: ['ani']
    },
    /* NO LICENSE 
    serbian: {
        document : "Колико имаш година?",
        terms: ['имаш']
    },*/
    /* NO LICENSE 
    slovak: {
        document : "Ako si starý ?",
        terms: ['starý']
    },*/
    /* NO LICENSE 
    slovenian: {
        document : "Koliko ste stari ?",
        terms: ['stari']
    },*/
    swedish: {
        document : "Hur gammal är du?",
        terms: ['gammal']
    },
    thai: {
        document : "คุณอายุเท่าไหร่ ?",
        terms: ['อายุ']
    },
    /* NO LICENSE 
    ukrainian: {
        document : "Скільки тобі років?",
        terms: ['тобі']
    },*/
};

(function () {

    Object.keys(languages).forEach(function (langCode) {

        db.ara.drop();

        print(langCode)

        db.ara.insert({ _id: 1, t1: languages[langCode].document });

        db.ara.ensureIndex({ t1: "text" }, { default_language: langCode });


        languages[langCode].terms.forEach(function (t1) {
            // Positive Term Match
            assert.eq([1], queryIds(db.ara, t1));

            // Positive Term Match
            //assert.eq(["past_writes", "future_writes"], queryIds(db.ara, "كتب"));

            // Phrase Match
            //assert.eq(["past_writes"], queryIds(db.ara, 'كتب "كَتَبْتُ"'));

            // Negative Phrase Match
            //assert.eq(["future_writes"], queryIds(db.ara, 'كتب -"كَتَبْتُ"'));
        });
    });

   
})();