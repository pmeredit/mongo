/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_look_up.h"

#include "mongo/base/init.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/stdx/memory.h"

namespace mongo {

using boost::intrusive_ptr;

DocumentSourceLookUp::DocumentSourceLookUp(NamespaceString fromNs,
                                           std::string as,
                                           std::string localField,
                                           std::string foreignField,
                                           const boost::intrusive_ptr<ExpressionContext>& pExpCtx)
    : DocumentSource(pExpCtx),
      _fromNs(std::move(fromNs)),
      _as(std::move(as)),
      _localField(std::move(localField)),
      _foreignField(foreignField),
      _foreignFieldFieldName(std::move(foreignField)) {}

REGISTER_DOCUMENT_SOURCE(lookUp, DocumentSourceLookUp::createFromBson);

const char* DocumentSourceLookUp::getSourceName() const {
    return "$lookUp";
}

boost::optional<Document> DocumentSourceLookUp::getNext() {
    pExpCtx->checkForInterrupt();

    uassert(4567, "from collection cannot be sharded", !_mongod->isSharded(_fromNs));

    if (_handlingUnwind) {
        return unwindResult();
    }

    boost::optional<Document> input = pSource->getNext();
    if (!input)
        return {};
    BSONObj query = queryForInput(*input);
    std::unique_ptr<DBClientCursor> cursor = _mongod->directClient()->query(_fromNs.ns(), query);

    std::vector<Value> results;
    int objsize = 0;
    while (cursor->more()) {
        BSONObj result = cursor->nextSafe();
        objsize += result.objsize();
        uassert(4568,
                str::stream() << "Total size of documents in " << _fromNs.coll() << " matching "
                              << query << " exceeds maximum document size",
                objsize <= BSONObjMaxInternalSize);
        results.push_back(Value(result));
    }

    MutableDocument output(std::move(*input));
    output.setNestedField(_as, Value(std::move(results)));
    return output.freeze();
}

bool DocumentSourceLookUp::coalesce(const intrusive_ptr<DocumentSource>& pNextSource) {
    if (_handlingUnwind) {
        return false;
    }

    auto unwindSrc = dynamic_cast<DocumentSourceUnwind*>(pNextSource.get());
    if (!unwindSrc || unwindSrc->getUnwindPath() != _as.getPath(false)) {
        return false;
    }
    _unwindSrc = std::move(unwindSrc);
    _handlingUnwind = true;
    return true;
}

void DocumentSourceLookUp::dispose() {
    _cursor.reset();
    pSource->dispose();
}

BSONObj DocumentSourceLookUp::queryForInput(const Document& input) const {
    Value localFieldVal = input.getNestedField(_localField);
    if (localFieldVal.missing()) {
        localFieldVal = Value(BSONNULL);
    }
    return BSON(_foreignFieldFieldName << localFieldVal);
}

boost::optional<Document> DocumentSourceLookUp::unwindResult() {
    // Skip input documents that have no matches and output one document per match rather than
    // building an array of matches, as that's what $unwind would do.
    while (!_cursor || !_cursor->more()) {
        _input = pSource->getNext();
        if (!_input)
            return {};
        _cursor = _mongod->directClient()->query(_fromNs.ns(), queryForInput(*_input));
    }

    // Move input document into output if this is the last or only result, otherwise perform a copy.
    MutableDocument output(_cursor->more() ? *_input : std::move(*_input));
    output.setNestedField(_as, Value(_cursor->nextSafe()));
    return output.freeze();
}

void DocumentSourceLookUp::serializeToArray(std::vector<Value>& array, bool explain) const {
    if (explain) {
        array.push_back(
            Value(DOC(getSourceName()
                      << DOC("from" << _fromNs.coll() << "as" << _as.getPath(false) << "localField"
                                    << _localField.getPath(false) << "foreignField"
                                    << _foreignField.getPath(false) << "unwinding"
                                    << (_handlingUnwind ? Value(true) : Value())))));
    } else {
        array.push_back(Value(
            DOC(getSourceName() << DOC("from" << _fromNs.coll() << "as" << _as.getPath(false)
                                              << "localField" << _localField.getPath(false)
                                              << "foreignField" << _foreignField.getPath(false)))));
        if (_handlingUnwind) {
            _unwindSrc->serializeToArray(array);
        }
    }
}

DocumentSource::GetDepsReturn DocumentSourceLookUp::getDependencies(DepsTracker* deps) const {
    deps->fields.insert(_localField.getPath(false));
    return SEE_NEXT;
}

boost::intrusive_ptr<DocumentSource> DocumentSourceLookUp::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(4569, "the $lookUp specification must be an Object", elem.type() == Object);

    NamespaceString fromNs;
    std::string as;
    std::string localField;
    std::string foreignField;

    for (auto&& argument : elem.Obj()) {
        uassert(4570,
                str::stream() << "arguments to $lookUp must be strings, " << argument << " is type "
                              << argument.type(),
                argument.type() == String);
        const auto argName = argument.fieldNameStringData();

        if (argName == "from") {
            fromNs = NamespaceString(pExpCtx->ns.db().toString() + '.' + argument.String());
        } else if (argName == "as") {
            as = argument.String();
        } else if (argName == "localField") {
            localField = argument.String();
        } else if (argName == "foreignField") {
            foreignField = argument.String();
        } else {
            uasserted(4571,
                      str::stream() << "unknown argument to $lookUp: " << argument.fieldName());
        }
    }

    uassert(4572,
            "need to specify fields from, as, localField, and foreignField for a $lookUp",
            !fromNs.ns().empty() && !as.empty() && !localField.empty() && !foreignField.empty());

    intrusive_ptr<DocumentSourceLookUp> lookUp(new DocumentSourceLookUp(
        std::move(fromNs), std::move(as), std::move(localField), std::move(foreignField), pExpCtx));
    return lookUp;
}
}
