//  Class that allows the mongo shell to talk to the mongodb KeyStore.
//  Loaded only into the enterprise module.

Mongo.prototype.getKeyStore = function() {
    return new KeyStore(this);
};

class KeyStore {
    constructor(mongo) {
        mongo.getDB("admin").createCollection("datakeys");
        this.keyColl = mongo.getCollection("admin.datakeys");
        this.keyColl.createIndex(
            {keyAltNames: 1},
            {unique: true, partialFilterExpression: {keyAltNames: {$exists: true}}});
    }

    createKey(keyAltNames = undefined) {
        var date = new Date();
        var data = "";
        var possible = "abcdef0123456789";

        for (var i = 0; i < 26; i++) {
            data += possible.charAt(Math.floor(Math.random() * possible.length));
        }

        // TODO: Delete the rest of the code and insert the KMS API call in the
        // line below. SERVER-39896

        var current = date.getDate();

        var masterKey = {
            "provider": "aws",
            "awsKey": data,
            "awsRegion": "USEAST",
        };

        var doc = {
            "_id": UUID(),
            "keyMaterial": data,
            "creationDate": current,
            "updateDate": current,
            "status": 0,
            "version": 0,
            "masterKey": masterKey,
        };

        if (keyAltNames) {
            if (!Array.isArray(keyAltNames)) {
                return "TypeError: key alternate names must be of Array type.";
            }

            for (i = 0; i < keyAltNames.length; i++) {
                if (typeof keyAltNames[i] !== "string") {
                    return "TypeError: items in key alternate names must be of String type.";
                }
            }

            doc.keyAltNames = keyAltNames;
        }

        return this.keyColl.insert(doc);
    }

    getKey(keyId) {
        return this.keyColl.find({"_id": keyId});
    }

    getKeyByAltName(keyAltName) {
        return this.keyColl.find({"keyAltNames": keyAltName});
    }

    deleteKey(keyId) {
        return this.keyColl.deleteOne({"_id": keyId});
    }

    getKeys() {
        return this.keyColl.find();
    }

    addKeyAlternateName(keyId, keyAltName) {
        // keyAltName is not allowed to be an array or an object. In javascript,
        // typeof array is object.
        if (typeof keyAltName === "object") {
            return "TypeError: key alternate name cannot be object or array type.";
        }
        return this.keyColl.findAndModify({
            query: {"_id": keyId},
            update: {$push: {"keyAltNames": keyAltName}, $currentDate: {"updateDate": true}},
        });
    }

    removeKeyAlternateName(keyId, keyAltName) {
        if (typeof keyAltName === "object") {
            return "TypeError: key alternate name cannot be object or array type.";
        }
        const ret = this.keyColl.findAndModify({
            query: {"_id": keyId},
            update: {$pull: {"keyAltNames": keyAltName}, $currentDate: {"updateDate": true}}
        });

        if (ret != null && ret.keyAltNames.length === 1 && ret.keyAltNames[0] === keyAltName) {
            // Remove the empty array to prevent duplicate key violations
            return this.keyColl.findAndModify({
                query: {"_id": keyId, "keyAltNames": undefined},
                update: {$unset: {"keyAltNames": ""}, $currentDate: {"updateDate": true}}
            });
        }
        return ret;
    }
}