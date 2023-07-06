// Data sourced from:
// https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests/find.json
// https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests/localKMS.json

export const keyvaultDataLocal = [{
    "status": NumberInt(0),
    "_id": BinData(4, "AAAAAAAAAAAAAAAAAAAAAA=="),
    "keyMaterial": BinData(
        0,
        "Ce9HSz/HKKGkIt4uyy+jDuKGA+rLC2cycykMo6vc8jXxqa1UVDYHWq1r+vZKbnnSRBfB981akzRKZCFpC05CTyFqDhXv6OnMjpG97OZEREGIsHEYiJkBW0jJJvfLLgeLsEpBzsro9FztGGXASxyxFRZFhXvHxyiLOKrdWfs7X1O/iK3pEoHMx6uSNSfUOgbebLfIqW7TO++iQS5g1xovXA=="),
    "creationDate": ISODate(),
    "updateDate": ISODate(),
    "masterKey": {"provider": "local"}
}];

export const keyvaultDataDecryption = [
    {
        "status": NumberInt(1),
        "_id": BinData(4, "LOCALAAAAAAAAAAAAAAAAA=="),
        "masterKey": {"provider": "local"},
        "updateDate": ISODate(),
        "keyMaterial": BinData(
            0,
            "Ce9HSz/HKKGkIt4uyy+jDuKGA+rLC2cycykMo6vc8jXxqa1UVDYHWq1r+vZKbnnSRBfB981akzRKZCFpC05CTyFqDhXv6OnMjpG97OZEREGIsHEYiJkBW0jJJvfLLgeLsEpBzsro9FztGGXASxyxFRZFhXvHxyiLOKrdWfs7X1O/iK3pEoHMx6uSNSfUOgbebLfIqW7TO++iQS5g1xovXA=="),
        "creationDate": ISODate(),
        "keyAltNames": ["local"]
    },
];

export const jsonSchema = {
    validator: {
        $jsonSchema: {
            "properties": {
                "encrypted_string": {
                    "encrypt": {
                        "keyId": [BinData(4, "AAAAAAAAAAAAAAAAAAAAAA==")],
                        "bsonType": "string",
                        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                    }
                },
                "encrypted_string_azure": {
                    "encrypt": {
                        "keyId": [BinData(4, "AZURE+AAAAAAAAAAAAAAAA==")],
                        "bsonType": "string",
                        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                    }
                },

                "encrypted_string_gcp": {
                    "encrypt": {
                        "keyId": [BinData(4, "GCP+AAAAAAAAAAAAAAAAAA==")],
                        "bsonType": "string",
                        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                    }
                },
            },
            "bsonType": "object"
        }
    }
};

export const providerObj = {
    local: {
        "key": BinData(
            0,
            "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk"),
    }
};

export const findDataLocal = [{
    "_id": 1,
    "encrypted_string": BinData(
        6,
        "AQAAAAAAAAAAAAAAAAAAAAACV/+zJmpqMU47yxS/xIVAviGi7wHDuFwaULAixEAoIh0xHz73UYOM3D8D44gcJn67EROjbz4ITpYzzlCJovDL0Q=="),
}];

export const binDataDecryption = [
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAABmvr0W8b4lcFbnXapyzbEJu7y4G8U1otHjvPRV3/fiz6kMVuBbgjxIXRK949oJUIpocbc4ZnMLBkZfpMQVqTgbA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAB99dGDJ8TVK0kqYsBKtfLeSU4MssJ6+ZDHVeNq34/el4eEk9hQBWpjBL/YWRh6ER3uXhgtxzfZ1X/XSvfFa7yBQ=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAABbgp4kP4Ff3AwfrEDGZqZESka9GQMuW3q8Cq4trgEZAZsvJy+y4zM0GV1P4rRDbDVWMPV8UwNB7PIUVuvn6tWgg=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAABsqOrgsXH981wjzuVKOGtBWLp5vy/m1Ol6ZaFulkNfCd3f5TDkwySIAUvGFu563w0EP6VW3jo7nXEpJHGwKpKsw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAACTKx6vmGR/zmEFI4Px41cIMLwA8u1Jx0ccrAtdM22lyoTuNf686a39yuhpwuU8nJTtIz/Hg7pMtwoZpnKS7pEfA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAACCVHp9AdklelnsZ3st41ssDo3teGLMan04EFG2tOLwLU4rza2zHmeVvU2FS8HcVXKSj2rkt1ybqUK5dm/ERAOsA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAC0t2t3agUqYNaF+rgJAse2cC5KXvJQkpLUfCFhtBmog4xg9V1+jD2hPXfG3juU1bR1l+obDRQAHHeZjJMj83+wg=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAACVGsEtPAu0mvY38RUREvxH5z9uUGV147riVYeex+ZNw7cCy3GVwOXmwb57DOYXKBC0ZYtLmMw82nHGW7CwgEYeg=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAACW0cZMYWOY3eoqQQkSdBtS9iHC4CSQA27dy6XJGcmTV8EDuhGNnPmbx0EKFTDb0PCSyCjMyuE4nsgmNYgjTaSuw=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAACW0cZMYWOY3eoqQQkSdBtS9iHC4CSQA27dy6XJGcmTV8EDuhGNnPmbx0EKFTDb0PCSyCjMyuE4nsgmNYgjTaSuw=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAACW0cZMYWOY3eoqQQkSdBtS9iHC4CSQA27dy6XJGcmTV8EDuhGNnPmbx0EKFTDb0PCSyCjMyuE4nsgmNYgjTaSuw=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAACW0cZMYWOY3eoqQQkSdBtS9iHC4CSQA27dy6XJGcmTV8EDuhGNnPmbx0EKFTDb0PCSyCjMyuE4nsgmNYgjTaSuw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAADudbNGKCfYi3BygueSE/rh5cwsueMKeny2lqqd0RinXaKN/QnEZZ8qiXTBOr5lDneOoO2qaLZ5WAhxXsvmLU3Mw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAADycKraFaktvZ1xKq97S1tLEGtV67zVZQxf3dm+FFFuZBzO4lpAh5WSJneisKiWlec3H4pWhfWnHAE6JQU+SLVYg=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAADpM5puNAXDa2im0l6dfiwYATwjKqChHOl4wJB2wRy3ql9XXR78c85qqcpfhY673wBrMlzO82sibQyll7K9f/F+g=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAADU41hRGpBQdMLy/v5U8TztoIKctPmkwPul99JkPva5tmWSN5MYolOQGkg5eEXHWg1OzC4ZRj/TU+DEJU3M3OYrA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAEA+IJjnU1TZJqUfpR/CWy5gc+ANmK99KaJ+ucpgtyZ7OD1eua3/2Ol9lmQ60NanhM1Qilsbh81G7yU0gLQPETLA5WwGEMtRXLSsiQGqtcxgQ='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAEDizByeeu0+4zo9Zb1TJzSbYADx4yBf8wtYl0F9/363THqng/zNP1Svkwc1tP5iqdkpCpBV5Fdc4msi3v2jPPqTA5W4z/JsnRyK5wHNqi6/s='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAEiVslbcKQ9o9J5uKGPsU1mCEjipIlRbEAvRi9i2nNV3debxMcxzxWFglaJxTyzNPOOq699cewfLnAA4sh3HDfuQBe4DJKfdFjxsl53/Tj29g='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAEgMMzSizfZVuWXpLbEBllZW1PCT8WjRoEi4Lq2tsGUbXqkhCGx3ttyAHTkvIoIjCuGFOt4cn2c/1gM/lRlZ3HFpIH8q6/uckZA1Wx3DeCWKA='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAFhkAEDOE/dXfJ1kXeW9+8Wh5HeyFOlaChdCv8sZHmnmnK1WXVLWxq73GwcwQSCLM0lPAcPvGFNFR7S2R2cCkkLA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAFLQpyxs5+OpSi24GOuv2eSzasDyQXbTRKi1hgKZmZTXVR3asRJo2r31/GkpmU4a+Wcr0Gsfy0A/qIDihQpXpv0w=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAFOW6kHS4tlXfonm7/6MWY3p+7e24GxrLGSfYgDyTsZ20tnE5HCOmu9UwcbWY3kKiHA5bwVW6rmKVQwVrlZRSpgw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAFyMyzBU/QrKEMwsLdRMzUnGcGM7AKpOsbaLCvd57VUQAxruRy+turzhRV4lygU0CQDM3Fc24hpF+0EXYgXfGOQg=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAF1ofBnK9+ERP29P/i14GQ/y3muic6tNKY532zCkzQkJSktYCOeXS8DdY1DdaOP/asZWzPTdgwby6/iZcAxJU+xQ=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAF1ofBnK9+ERP29P/i14GQ/y3muic6tNKY532zCkzQkJSktYCOeXS8DdY1DdaOP/asZWzPTdgwby6/iZcAxJU+xQ=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAF1ofBnK9+ERP29P/i14GQ/y3muic6tNKY532zCkzQkJSktYCOeXS8DdY1DdaOP/asZWzPTdgwby6/iZcAxJU+xQ=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAF1ofBnK9+ERP29P/i14GQ/y3muic6tNKY532zCkzQkJSktYCOeXS8DdY1DdaOP/asZWzPTdgwby6/iZcAxJU+xQ=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAFINmvibUNjhJJc44opxWMqjXzKxzYZor7VbZ3Edx+f8wQ0iHAwMeLGMTMBK1YWbnd52CPxGChoL+g6ZNqG0kM6zQhnnR5pmWINxvJvf5OdbA='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAF1BVFZay3E2uddcBcvq2vAhPOQPocuKJyP9bDs8TaRkM9REUmxrVPDwrAeCMMDNvaeyjLZEagJJjMUEKbUB2JKCdUIxFx8uIRfXtmZIsIC7o='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAFvGF57yU4rdsN5WD7NKxPFPu93PAfsJSHXusgsI8WUWFLx5JZKVVVmlbtgi+EYBoLwO3W/MYsYaBzH1jxTSIhWsPDWi4BMKXpVhVVWFTb/zU='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAFxoY1RO9WKAJZkv4Q14GWwZZMSTswhA0PFJLTqsgBivwJHdneD4B5drmBPclxSDuBpWRBXfUJibWwdKg5RvL7qAoGXphKrSBbK8GIhDkyD4A='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAFwO3hsD8ee/uwgUiHWem8fGe54LsTJWqgbRCacIe6sxrsyLT6EsVIqg4Sn7Ou+FC3WJbFld5kx8euLe/MHa8FGYjxD97z5j+rUx5tt3T6YbA='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAFwO3hsD8ee/uwgUiHWem8fGe54LsTJWqgbRCacIe6sxrsyLT6EsVIqg4Sn7Ou+FC3WJbFld5kx8euLe/MHa8FGYjxD97z5j+rUx5tt3T6YbA='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAFwO3hsD8ee/uwgUiHWem8fGe54LsTJWqgbRCacIe6sxrsyLT6EsVIqg4Sn7Ou+FC3WJbFld5kx8euLe/MHa8FGYjxD97z5j+rUx5tt3T6YbA='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAFwO3hsD8ee/uwgUiHWem8fGe54LsTJWqgbRCacIe6sxrsyLT6EsVIqg4Sn7Ou+FC3WJbFld5kx8euLe/MHa8FGYjxD97z5j+rUx5tt3T6YbA='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAH97Ui/rLmEF4MqHgchzLfsK3EXJHGaER3NsjV52CYsJjhhvhpHlLCav5iDKRyKjaaKzU3yFNmDOY5fF6VtnXQHw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAHOrYvleXpCX3IIZIv4p+4PSex4AYpzgyg73B8XjJtceLkth9e68yyVnuiLRiTaF4VGdOCHDU9rvDTUAGso0iQmQ=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAHv4eDycUMEJAFYs6o1A52NDRXb9jMtgK9b8BA8WXYSC048KCRp6PvhIis60I4UfX0h0btEIfQaA+GQ3MnjYpPrQ=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAHgkRKWeeNfMh2xDtSyrLYGJx2PFz0yXaVCpQZbBRqoae7zYNRqWC3NG7OsJ5kc/thqD79+OR8hPxnVyG+DGFq8w=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAH4ElF4AvQ+kkGfhadgKNy3GcYrDZPN6RpzaMYIhcCGDvC9W+cIS9dH1aJbPU7vTPmEZnnynPTDWjw3rAj2+9mOA=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAH4ElF4AvQ+kkGfhadgKNy3GcYrDZPN6RpzaMYIhcCGDvC9W+cIS9dH1aJbPU7vTPmEZnnynPTDWjw3rAj2+9mOA=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAH4ElF4AvQ+kkGfhadgKNy3GcYrDZPN6RpzaMYIhcCGDvC9W+cIS9dH1aJbPU7vTPmEZnnynPTDWjw3rAj2+9mOA=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAH4ElF4AvQ+kkGfhadgKNy3GcYrDZPN6RpzaMYIhcCGDvC9W+cIS9dH1aJbPU7vTPmEZnnynPTDWjw3rAj2+9mOA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAIFk1Obh42QWEFgTh29A/3Ez5/jg8DbhnI18KsAj20TB74m/wE2gp5DWnCpvGBoMY5ozyQLJXUFyFFbPQdqvTDaQ=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAIDKEGJYD1/NbXSH6XvdgH/yVopabDitj98D/X6bt0PrYViJa2mfSyj2RvAA5COlIqBINyVcp27oncQy+bzyyVLg=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAIPolPWoxyjTPClLHG21oWVnOWfI/MpEiZ+E98xzJzFLZw+pcFTizDpXXIJC1rq5HasqypPk2fKaoDGMT85f7n0Q=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAIgSV+ue2UeUjqPDRaqGvCdUe525/bfRHuKO4310gsh2vjkBhU1DQlPBZbi9QWyxpY/p1laTP79bIiOQ47f0A0qA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAJYruWq79Ry5UCXZBLu46kY4mG94oZH+MMgzoTEi22ZQfgBV+W39mZuOVBqdYOnB9Oiw51FDUpjL3LAdoL3TYP4A=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAJBv0oMbcjdsmRmvjEALX/JqeB7F7IGtDHpNLRYjZQJwvZdjz0WUHjjfXbasCNOLzAca+cIDyWiF7DJZxa0VVYGQ=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAJsUPpcAlLrhweEoj5NKXnkIUcENVQmvMCNitMVADkXNiBSFxpHiNtwSuvyBgQSiKgQeB5vK9unpBeXBA2tvqn3Q=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAJLqk8gH+wcWJHMlW5lW2K7lzSS32K75PlFEoehIGBIQLbWx0SoEifcXYhe08rf2ZPY9JoIAyyeV8D4aJaeJRfww=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAJ1GMYQTruoKr6fv9XCbcVkx/3yivymPSMEkPCRDYxQv45w4TqBKMDfpRd1TOLOv1qvcb+gjH+z5IfVBMp2IpG/Q=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAJ1GMYQTruoKr6fv9XCbcVkx/3yivymPSMEkPCRDYxQv45w4TqBKMDfpRd1TOLOv1qvcb+gjH+z5IfVBMp2IpG/Q=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAJ1GMYQTruoKr6fv9XCbcVkx/3yivymPSMEkPCRDYxQv45w4TqBKMDfpRd1TOLOv1qvcb+gjH+z5IfVBMp2IpG/Q=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAJ1GMYQTruoKr6fv9XCbcVkx/3yivymPSMEkPCRDYxQv45w4TqBKMDfpRd1TOLOv1qvcb+gjH+z5IfVBMp2IpG/Q=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAALSNaki59phgFtPQ8Hsg15EAozEWcPQUkogqwx9oEaMVyBHX3CbTVMBxKRLJxnazl6foG5wCC0gzaRlKkE34r/Ng=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAALDGRh0/inBXP6UIHYKTczT73y2l79vui1pdxvMJfG0w1IHYlQivcNNN6GtR4pzsjBJMi53mzpSfCDVhTTlr21iA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAALQFDojn1lEboUzgIzfIgKnF7l0/i7cVN0IIDkvsCkjjKNEH5xxccS51VBgrGky2sHwDdwWzE9WmW4+aTFfuT1BA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAALxgoelr9Rt/n1D7o/2PErZGu5q9wZg7SuNFSSV0bclCisXx0jCCcOlUr3MoqleJLamQ9DT7/Kqkn0DBJgEGA8kw=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAALiZbL5nFIZl7cSLH5E3wK3jJeAeFc7hLHNITtLAu+o10raEs5i/UCihMHmkf8KHZxghs056pfm5BjPzlL9x7IHQ=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAALiZbL5nFIZl7cSLH5E3wK3jJeAeFc7hLHNITtLAu+o10raEs5i/UCihMHmkf8KHZxghs056pfm5BjPzlL9x7IHQ=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAALiZbL5nFIZl7cSLH5E3wK3jJeAeFc7hLHNITtLAu+o10raEs5i/UCihMHmkf8KHZxghs056pfm5BjPzlL9x7IHQ=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAALiZbL5nFIZl7cSLH5E3wK3jJeAeFc7hLHNITtLAu+o10raEs5i/UCihMHmkf8KHZxghs056pfm5BjPzlL9x7IHQ=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAMXZyQVKL4GDdVG/FsbkRLOqA74BGvD1gbveAeePzalhG68xM5avdLO8+xA4hfMHb3C0ODymimUESLWHReh8zhZwXQQCdaElXWZGUzUwAM6PM='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAMeepzb1RxqM/ruTrasvfRVfIDUHTG19P54vydPxsa5h/gnV6C/YTz0a3fBeOF89zWj3ppR9te4OHTLxYx+QI+qakfd6BVtJq+uIhMskNo9zY='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAMu6vZt44pVcOIp3ebmg+3vjvcXzQqIkLJu/J9lgqg7vlJmODIpuCzMSExNVS/HJvtjNyOWUsv+X+nIWEvc8PvN4tA+B1YPhsuj0/++HSExcE='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAM1wu9kpWtPgbQtPOy81L6q0BzZ+z2OTVbzL5XgWjUvs4nKbwmipvTA3V2y2qYCofJewAaypjvPVD4Ub9Cqe2bqhFF3Ro0n5jT/Kgmn5bWqKo='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAMQWace2C1w3yqtmo/rgz3YtIDnx1Ia/oDsoHnnMZlEy5RoK3uosi1hvNAZCSg3Sen0H7MH3XVhGGMCL4cS69uJ0ENSvh+K6fiZzAXCKUPfvM='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAMQWace2C1w3yqtmo/rgz3YtIDnx1Ia/oDsoHnnMZlEy5RoK3uosi1hvNAZCSg3Sen0H7MH3XVhGGMCL4cS69uJ0ENSvh+K6fiZzAXCKUPfvM='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAMQWace2C1w3yqtmo/rgz3YtIDnx1Ia/oDsoHnnMZlEy5RoK3uosi1hvNAZCSg3Sen0H7MH3XVhGGMCL4cS69uJ0ENSvh+K6fiZzAXCKUPfvM='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAMQWace2C1w3yqtmo/rgz3YtIDnx1Ia/oDsoHnnMZlEy5RoK3uosi1hvNAZCSg3Sen0H7MH3XVhGGMCL4cS69uJ0ENSvh+K6fiZzAXCKUPfvM='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAANObKIAo1Z9Rhvyi67fx1FUMyP+H+IAWIM+phCne/mi8gXyE5KI+UjRoMUqwEWIf4hF7x1PLLBLOnv9vjYOxhOPw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAANFuImM+TZ7t7fcNuASVPI7lTG76j1wtZWbiL/2vbJLSXH6vQscZeTAvkjcaTY35XA/BvXZG2JJj7jEmC82RyTAg=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAANFOKgXsXaOrdDoLQkmWtiZJ75sA1uU+3iMH+bbxmRdR7GTluYxqek9ssYQQeW1XaLDCSHbrk+fvZ4YYBav4Tdjw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAANyc/K8k5iP42mNC991m7D2WKSZ6io3F7ryh6XLJ0MO4BSg8GebHjrs48sGfFqIesOKjsyuOlEBHOCIVQI5wpNTg=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAANmQsg9E/BzGJVNVhSNyunS/TH0332oVFdPS6gjX0Cp/JC0YhB97DLz3N4e/q8ECaz7tTdQt9JacNUgxo+YCULUA=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAANmQsg9E/BzGJVNVhSNyunS/TH0332oVFdPS6gjX0Cp/JC0YhB97DLz3N4e/q8ECaz7tTdQt9JacNUgxo+YCULUA=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAANmQsg9E/BzGJVNVhSNyunS/TH0332oVFdPS6gjX0Cp/JC0YhB97DLz3N4e/q8ECaz7tTdQt9JacNUgxo+YCULUA=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAANmQsg9E/BzGJVNVhSNyunS/TH0332oVFdPS6gjX0Cp/JC0YhB97DLz3N4e/q8ECaz7tTdQt9JacNUgxo+YCULUA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAOxMUOVXQrSgvyzgD+xBAn3p3xxzQf8olvi+QqsL50loXdt3rL16liFJzo19m4Aw9usFMVtdxKxoka+/94Jdl24Uh9yHh4lNy3DfGi0esX8WA='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAOcggAd1JJ3ekPqd3CevAu26+UKTt2GZdNKSSZSRou2GzVtiV8/d0lj8saTMz3DZ+mLXZV5K/9d+zhRQ2qJtwFCTjwZ3w6smhUhKBFIVxHg4g='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAOKRw3xdSzSGV0bAXIYpBFPW8cxgOX0MTC+xSj5HhiC2nwN9wZ1YuXX6h3Cd0LOzYfVuby4Q+6foPr/3eHkYe1ArcUO8IDKyCeKleGzBtDn6E='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAOmNF4cGG3PT9vqHfAcYwGFbtZWAjifbLGUtXmrVTJ41T/QR9ptSCfh+FamP2ySKTV1y4bfdcbV830mS3GnFghl47l7E2+33UdbTbrQAAn5GM='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAOsg5cs6VpZWoTOFg4ztZmpj8kSTeCArVcI1Zz2pOnmMqNv/vcKQGhKSBbfniMripr7iuiYtlgkHGsdO2FqUp6Jb8NEWm5uWqdNU21zR9SRkE='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAOsg5cs6VpZWoTOFg4ztZmpj8kSTeCArVcI1Zz2pOnmMqNv/vcKQGhKSBbfniMripr7iuiYtlgkHGsdO2FqUp6Jb8NEWm5uWqdNU21zR9SRkE='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAOsg5cs6VpZWoTOFg4ztZmpj8kSTeCArVcI1Zz2pOnmMqNv/vcKQGhKSBbfniMripr7iuiYtlgkHGsdO2FqUp6Jb8NEWm5uWqdNU21zR9SRkE='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAOsg5cs6VpZWoTOFg4ztZmpj8kSTeCArVcI1Zz2pOnmMqNv/vcKQGhKSBbfniMripr7iuiYtlgkHGsdO2FqUp6Jb8NEWm5uWqdNU21zR9SRkE='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAPWs+QOHRP/BI/FsVpKSvTf9S2w3CSl6oWuw5T2657iaEvtfYqa4YyhDIQ0STqXDynN0tFx8jr+NRmcr4n6x/KjDylx4qf9tBHd2g3bivZ25E='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAP4mweXl2pP+8ccFfktCbGdb6zBnGT8R2n9VtMDxkkuMuXHzJ8vPrR3axj++fIzS/FnEZoIJ0MqSGsgdeqlp1sEbysNejy1kNSLt63qowIw5k='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAPF34OOm/uBjhGsSKdML3a01DY1gaH8fdqW1cM7HwGVX1M9iqW1K7hnC3IJcOfHb1Yb7XCihHm2B+I0VSD9xvo/Lj8wF7soB1OMtf6JyLtHfA='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAPI+bEoEcC8lN9/rH9fTafKHyEAeD8lszfhCpckCru/TeSDgqyU8FgBmnp/wnnWfB88wDBtZa5gGMOgY9q2ldGeKqrUOwlxurXCULqvax/HfQ='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAQwWkdDxTo6h2+Cpl3zFyNUOhhA/xzQSTLq92iuEPqwQyKOWGURrHyrb3Uxs0a9VTa4pboufpCRVy56JLxnLVcgQ=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAQOmGXBr/HqmCmXQf2VA6TS8kBbeZ/zJQ/nL2PXIbPM+UwkI0sMBmrHy1dE5aG7LczlEWKg3ZBwDPya1SxLz3NUw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAQ8ZKIibm9Gl0+rkZ+M53d8ibsPtZchp+N3LRHEJWCqPTf6lnc/BffUzFFEyaR7O7qPiFxfI9ayCXhZrQIpT4c8g=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAQh+1sCiOVVL5H/rwqdSV5yjQEqF77DLKkegtU3bhBDrRgA0fJx0dtZopZcgCJmBRoEaEx8S5w5KKDRwVNXYdUHQ=='),
    BinData(6,
            'ASzggCwAAAAAAAAAAAAAAAAQIxWjLBromNUgiOoeoZ4RUJUYIfhfOmab0sa4qYlS9bgYI41FU6BtzaOevR16O9i+uACbiHL0X6FMXKjOmiRAug=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAQIxWjLBromNUgiOoeoZ4RUJUYIfhfOmab0sa4qYlS9bgYI41FU6BtzaOevR16O9i+uACbiHL0X6FMXKjOmiRAug=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAQIxWjLBromNUgiOoeoZ4RUJUYIfhfOmab0sa4qYlS9bgYI41FU6BtzaOevR16O9i+uACbiHL0X6FMXKjOmiRAug=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAQIxWjLBromNUgiOoeoZ4RUJUYIfhfOmab0sa4qYlS9bgYI41FU6BtzaOevR16O9i+uACbiHL0X6FMXKjOmiRAug=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAARKYzCdugZj3kBper+3mKsYTMoC/6qLUQw5bzqKiiBBqHgVBGLgDAG56inGd4YhGqVZd2zmoa7IoJu74F2w25olg=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAARYkH+9O04GQuqgdAXDtXEjxCNTRDPy6p8DojIvxsXRPqS4QimJstIN5uaLlwvZQ5+DuW0Bijlc99ULziuCK+/gA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAARNzl20yUsog4zoKPh0/dHvbI0OyTFBpm1N3qwz08i7DaZeNOPguP9tZeOkeQ9gZH/cXD83M94OeB+P/LFTj89Aw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAARtvx46e9auVI0U5pCfWj0LTmFJAvwZ+HbiTbZL23cpQuMaTNKjBKdZq/yafpOSSVKhEzI9+Uj2X8/jWTvq7udRA=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAR6uMylGytMq8QDr5Yz3w9HlW2MkGt6yIgUKcXYSaXru8eer+EkLv66/vy5rHqTfV0+8ryoi+d+PWO5U6b3Ng5Gg=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAR6uMylGytMq8QDr5Yz3w9HlW2MkGt6yIgUKcXYSaXru8eer+EkLv66/vy5rHqTfV0+8ryoi+d+PWO5U6b3Ng5Gg=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAR6uMylGytMq8QDr5Yz3w9HlW2MkGt6yIgUKcXYSaXru8eer+EkLv66/vy5rHqTfV0+8ryoi+d+PWO5U6b3Ng5Gg=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAAR6uMylGytMq8QDr5Yz3w9HlW2MkGt6yIgUKcXYSaXru8eer+EkLv66/vy5rHqTfV0+8ryoi+d+PWO5U6b3Ng5Gg=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAASSVa42/WyOA2SGX2m2KcqMEkWN/l5+qv+jl9arofcVfeX6qkeuqM+DC/9ziJkvsF/7s9DI2QhbjJNEbj2mJDkKQ=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAASzL1dK2FMTMLYxP6axCHiV3ikwlA3u5zKGF3+wM/sxyprl3mjcCi1NwkFyQH9YA/pQ9H+ClXEIfC98TX89QHqmA=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAASncR/wRkX/HLn2+18GNj3pN8Br3ajnPhTk4VhZbgiZgyEmKquaPN24u7NQd5vL7pg3eKcYNAVxFcMr9rm091kDg=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAASeLVwHdT8yHoBg5atURT7oucAM+YjxD6HqfYfNVg4A05uUCf2VnxUWlWNHObYJDi/CbI5gYnR5aryPFDzD1vCyA=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAASQk372m/hW3WX82/GH+ikPv3QUwK7Hh/RBpAguiNxMdNhkgA/y2gznVNm17t6djyub7+d5zN4P5PLS/EOm2kjtw=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAASQk372m/hW3WX82/GH+ikPv3QUwK7Hh/RBpAguiNxMdNhkgA/y2gznVNm17t6djyub7+d5zN4P5PLS/EOm2kjtw=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAASQk372m/hW3WX82/GH+ikPv3QUwK7Hh/RBpAguiNxMdNhkgA/y2gznVNm17t6djyub7+d5zN4P5PLS/EOm2kjtw=='),
    BinData(
        6,
        'ASzggCwAAAAAAAAAAAAAAAASQk372m/hW3WX82/GH+ikPv3QUwK7Hh/RBpAguiNxMdNhkgA/y2gznVNm17t6djyub7+d5zN4P5PLS/EOm2kjtw=='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAATPNuEyaZJ3SKQjzlvozpriBP2jdReFbxMzwv9NwtwkRAtSQSDF5Y6mAiCKFGOZ8BhlDCzGlpbZ6XMDB0rsIfJsONN4IQB8Au0gnBZ+zboS+0='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAT5XsdplzQbvijs7dpEbfWuPI05KWKUokLSklR6aN+a7OhUg1HCxAc5yQeWNknHnNK9D/u1eibg8gJA2L+0oIIyyTKImx2ri+UuNpSnjEu6Tw='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAAT1vTTReF5uCVaAUMDY2Iipa347xnAXMcg7Kq3lLlPSy+5yOsqWSEnNMwBoazMujDjaZxJrPPfoR2p9eP5emxoR3JAu5sG34ulBeZ63SMoJ0A='),
    BinData(
        6,
        'AizggCwAAAAAAAAAAAAAAAATdODPCn1O+HwzTCDIYtBNj/GhYcZCOfZ7ZlufjyNHcVxW8+H7FQnNlRJLHAYsUgaaTNaQr/muzQ7eTKUh0y63KGtOxsf+lfDoP5L1lrV/L14=')
];
