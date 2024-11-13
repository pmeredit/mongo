/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "audit/audit_header_options_gen.h"
#include "audit_decryptor_options.h"
#include "audit_enc_comp_manager.h"
#include "audit_key_manager_kmip.h"
#include "audit_key_manager_local.h"

#include "encryptdb/encryption_options.h"

#include "mongo/bson/json.h"
#include "mongo/db/service_context.h"
#include "mongo/util/base64.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/text.h"

namespace mongo {
namespace audit {
namespace {

StatusWith<AuditHeaderOptionsDocument> parseAuditHeaderFromJSON(const std::string& header) try {
    BSONObj fileHeaderBSON = fromjson(header);
    IDLParserContext ctx("Audit file header");
    return AuditHeaderOptionsDocument::parse(ctx, fileHeaderBSON);
} catch (const DBException& e) {
    return e.toStatus().withContext("Audit file header has invalid format");
}

StatusWith<EncryptedAuditFrame> getEncryptedAuditFrame(const std::string& line) try {
    EncryptedAuditFrame frame;
    BSONObj bson = fromjson(line);
    frame.ts = bson.getField(EncryptedAuditFrame::kTimestampField).Date();
    auto str = bson.getStringField(EncryptedAuditFrame::kLogField);
    frame.payload = std::vector(reinterpret_cast<const uint8_t*>(str.rawData()),
                                reinterpret_cast<const uint8_t*>(str.rawData()) + str.size());
    return frame;
} catch (...) {
    return Status{ErrorCodes::BadValue, "Could not get compressed message."};
}

std::unique_ptr<AuditKeyManager> createKeyManagerFromHeader(
    const AuditHeaderOptionsDocument& header) {
    BSONObj ks = header.getKeyStoreIdentifier();

    StringData type = ks.getStringField("provider"_sd);

    if (type == "local"_sd) {
        StringData file = ks.getStringField("filename"_sd);
        uassert(ErrorCodes::BadValue,
                "Audit file header has local key store with empty filename",
                !file.empty());
        return std::make_unique<AuditKeyManagerLocal>(file);
    } else if (type == "kmip"_sd) {
        StringData method = ks.getStringField("keyWrapMethod"_sd);
        StringData uid = ks.getStringField("uid"_sd);
        if (method == "encrypt"_sd) {
            uassert(
                ErrorCodes::BadValue,
                "Attempting to decrypt an audit log encrypted with the KMIP 1.2 key manager, but "
                "security.kmip.useLegacyProtocol is set to true, forcing the use of the KMIP 1.0 "
                "protocol. Please set "
                "security.kmip.useLegacyProtocol to false and ensure that the KMIP server is "
                "running "
                "at least protocol version 1.2 to decrypt this log",
                encryptionGlobalParams.kmipParams.version[0] >= 1 &&
                    encryptionGlobalParams.kmipParams.version[1] >= 2);
            return std::make_unique<AuditKeyManagerKMIPEncrypt>(
                uid.toString(), KeyStoreIDFormat::kmipKeyIdentifier);
        } else if (method == "get"_sd) {
            return std::make_unique<AuditKeyManagerKMIPGet>(uid.toString());
        }
        uasserted(ErrorCodes::BadValue,
                  str::stream() << "Audit file header has invalid KMIP keyWrapMethod: " << method
                                << ".");
    } else {
        uasserted(ErrorCodes::BadValue, "Audit file header has invalid key store identifier");
    }
}

void auditDecryptorTool(int argc, char* argv[]) try {
    setupSignalHandlers();
    runGlobalInitializersOrDie(std::vector<std::string>(argv, argv + argc));
    startSignalProcessingThread();

    setGlobalServiceContext(mongo::ServiceContext::make());

    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "The file '" << globalAuditDecryptorOptions.inputPath.string()
                          << "' could not be found.",
            boost::filesystem::exists(globalAuditDecryptorOptions.inputPath));

    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "The file '" << globalAuditDecryptorOptions.inputPath.string()
                          << "' is not a regular file.",
            boost::filesystem::is_regular_file(globalAuditDecryptorOptions.inputPath));

    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "The file '" << globalAuditDecryptorOptions.outputPath.string()
                          << "' already exists. Refusing to overwrite.",
            !boost::filesystem::exists(globalAuditDecryptorOptions.outputPath));

    if (globalAuditDecryptorOptions.confirmDecryption) {
        std::cout << "The following file is about to be decrypted: "
                  << globalAuditDecryptorOptions.inputPath << std::endl
                  << "The decrypted contents will be placed in: "
                  << globalAuditDecryptorOptions.outputPath << std::endl
                  << "Please be aware that this operation may potentially place sensitive "
                     "information "
                  << "in clear text on your filesystem." << std::endl
                  << std::endl
                  << "Do you wish to continue? (Yes/No) [Default: No] ";
        std::string userConfirmation;
        std::getline(std::cin, userConfirmation);
        std::transform(
            userConfirmation.begin(), userConfirmation.end(), userConfirmation.begin(), ::tolower);
        if (userConfirmation != "yes" && userConfirmation != "y") {
            std::cout << "Not continuing." << std::endl;
            quickExit(ExitCode::clean);
        }
    }

    // Open input file
    std::ifstream input(globalAuditDecryptorOptions.inputPath.c_str(), std::ios::in);
    std::string line;
    std::string header;
    std::vector<BSONObj> inputData;
    int numLine = 0;

    // Read file
    if (input.is_open()) {

        std::getline(input, header);
        auto headerObj = uassertStatusOK(parseAuditHeaderFromJSON(header));

        auto encKey = headerObj.getEncryptedKey();
        AuditKeyManager::WrappedKey wrappedKey(encKey.data<std::uint8_t>(),
                                               encKey.data<std::uint8_t>() + encKey.length());

        auto keyManager = createKeyManagerFromHeader(headerObj);

        AuditEncryptionCompressionManager ac = AuditEncryptionCompressionManager(
            std::move(keyManager), headerObj.getCompressionMode() == "zstd", &wrappedKey);

        uassertStatusOK(ac.verifyHeaderMAC(headerObj));
        ac.setSequenceIDCheckerFromHeader(headerObj);

        while (getline(input, line)) {
            ++numLine;
            try {
                EncryptedAuditFrame frame = uassertStatusOK(getEncryptedAuditFrame(line));
                inputData.push_back(ac.decryptAndDecompress(frame));
            } catch (...) {
                uassert(ErrorCodes::FileStreamFailed,
                        str::stream() << "Failed to decrypt line #" << numLine
                                      << ". Contents were: " << line << ".",
                        false);
            }
        }
        input.close();
    } else {
        uassert(ErrorCodes::FileOpenFailed,
                str::stream() << "Failed to open encrypted file '"
                              << globalAuditDecryptorOptions.inputPath.string() << "'",
                false);
    }

    std::ofstream output(globalAuditDecryptorOptions.outputPath.c_str(),
                         std::ios::out | std::ios::binary);

    uassert(ErrorCodes::FileOpenFailed,
            str::stream() << "Failed to open output file '"
                          << globalAuditDecryptorOptions.outputPath.string() << "'",
            output.is_open());

    uassert(ErrorCodes::FileStreamFailed, "Read no data.", !inputData.empty());

    // Write out cleartext
    output << header << std::endl;
    std::for_each(inputData.begin(), inputData.end(), [&](auto& obj) {
        output.write(obj.objdata(), obj.objsize());
    });

    std::cout << "Successfully decrypted " << numLine << " lines to '"
              << globalAuditDecryptorOptions.outputPath << "'." << std::endl;

    // To avoid Static Initialization Order Fiasco during exit, use quickExit to avoid static
    // destructors
    mongo::quickExit(ExitCode::clean);
} catch (...) {
    auto cause = exceptionToStatus();
    std::cerr << cause << std::endl;
    quickExit(ExitCode::fail);
}
}  // namespace
}  // namespace audit
}  // namespace mongo

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through argv().  This enables auditDecryptorTool()
// to process UTF-8 encoded arguments without regard to platform.
int wmain(int argc, wchar_t** argvW) {
    mongo::WindowsCommandLine wcl(argc, argvW);
    mongo::audit::auditDecryptorTool(argc, wcl.argv());
}
#else
int main(int argc, char** argv) {
    mongo::audit::auditDecryptorTool(argc, argv);
}
#endif
