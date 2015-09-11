/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/optional.hpp>
#include <string>

#include "mongo/db/jsobj.h"
#include "mongo/db/pipeline/dependencies.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/value.h"


namespace mongo {

/**
 * Queries separate collection for equality matches with documents in the pipeline collection.
 * Adds matching documents to a new array field in the input document.
 */
class DocumentSourceLookUp final : public DocumentSource,
                                   public SplittableDocumentSource,
                                   public DocumentSourceNeedsMongod {
public:
    boost::optional<Document> getNext() final;
    const char* getSourceName() const final;
    bool coalesce(const boost::intrusive_ptr<DocumentSource>& pNextSource) final;
    void serializeToArray(std::vector<Value>& array, bool explain = false) const final;
    GetDepsReturn getDependencies(DepsTracker* deps) const final;
    void dispose() final;

    bool needsPrimaryShard() const final {
        return true;
    }

    boost::intrusive_ptr<DocumentSource> getShardSource() final {
        return nullptr;
    }

    boost::intrusive_ptr<DocumentSource> getMergeSource() final {
        return this;
    }

    void addInvolvedCollections(std::vector<NamespaceString>* collections) const final {
        collections->push_back(_fromNs);
    }

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

private:
    DocumentSourceLookUp(NamespaceString fromNs,
                         std::string as,
                         std::string localField,
                         std::string foreignField,
                         const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    Value serialize(bool explain = false) const final {
        invariant(false);
    }

    boost::optional<Document> unwindResult();
    BSONObj queryForInput(const Document& input) const;

    NamespaceString _fromNs;
    FieldPath _as;
    FieldPath _localField;
    FieldPath _foreignField;
    std::string _foreignFieldFieldName;

    boost::intrusive_ptr<DocumentSourceUnwind> _unwindSrc;
    bool _handlingUnwind = false;
    std::unique_ptr<DBClientCursor> _cursor;
    long long _cursorIndex = 0;
    boost::optional<Document> _input;
};
}
