/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "document_source_backup_file.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

namespace mongo {

REGISTER_DOCUMENT_SOURCE_CONDITIONALLY(
    _backupFile,
    LiteParsedDocumentSourceDefault::parse,
    DocumentSourceBackupFile::createFromBson,
    AllowedWithApiStrict::kInternal,
    AllowedWithClientType::kInternal,
    boost::none,
    repl::feature_flags::gFileCopyBasedInitialSync.isEnabledAndIgnoreFCV());

boost::intrusive_ptr<DocumentSourceBackupFile> DocumentSourceBackupFile::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, DocumentSourceBackupFileSpec spec) {
    return make_intrusive<DocumentSourceBackupFile>(expCtx, std::move(spec));
}

boost::intrusive_ptr<DocumentSourceBackupFile> DocumentSourceBackupFile::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {

    uassert(ErrorCodes::FailedToParse,
            str::stream() << kStageName
                          << " value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);


    auto spec = DocumentSourceBackupFileSpec::parse(IDLParserErrorContext(kStageName),
                                                    elem.embeddedObject());

    return DocumentSourceBackupFile::create(expCtx, spec);
}

DocumentSource::GetNextResult DocumentSourceBackupFile::doGetNext() {
    // TODO SERVER-57810: Implement reading of filenames
    return pSource->getNext();
}

DocumentSourceBackupFile::DocumentSourceBackupFile(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, DocumentSourceBackupFileSpec spec)
    : DocumentSource(kStageName, expCtx), _backupFileSpec(std::move(spec)) {}

Value DocumentSourceBackupFile::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value();
}

}  // namespace mongo
