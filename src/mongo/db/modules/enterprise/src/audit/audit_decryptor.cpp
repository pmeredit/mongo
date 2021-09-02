/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "audit_decryptor_options.h"
#include "audit_enc_comp_manager.h"

#include "mongo/bson/json.h"
#include "mongo/db/service_context.h"
#include "mongo/util/base64.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/text.h"

namespace mongo {
namespace audit {

bool isValidHeader(const std::string& header) {
    std::string properties[] = {"ts",
                                "version",
                                "compressionMode",
                                "keyStoreIdentifier",
                                "encryptedKey",
                                "auditRecordType"};
    try {
        BSONObj fileHeaderBSON = fromjson(header);
        for (std::string prop : properties) {
            if (!fileHeaderBSON.hasField(prop)) {
                return false;
            }
        }
    } catch (...) {
        return false;
    }

    return true;
}

StatusWith<std::string> getCompressedMessage(const std::string& line) {
    try {
        BSONObj fileHeaderBSON = fromjson(line);
        return fileHeaderBSON.getStringField("log");
    } catch (...) {
        return Status{ErrorCodes::BadValue, str::stream() << "Could not get compressed message."};
    }
}

void auditDecryptorTool(int argc, char* argv[]) {
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
                          << "' was not a regular file.",
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
        uassert(ErrorCodes::OK,
                "Not continuing.",
                userConfirmation == "yes" || userConfirmation == "Yes" || userConfirmation == "y" ||
                    userConfirmation == "Y");
    }

    // Open input file
    std::ifstream input(globalAuditDecryptorOptions.inputPath.c_str(), std::ios::in);
    std::string line;
    std::string header;
    std::vector<BSONObj> inputData;
    int numLine = 0;

    // Read file
    if (input.is_open()) {
        getline(input, line);
        uassert(ErrorCodes::FailedToParse,
                "Audit file header has invalid format.",
                isValidHeader(line));
        header.swap(line);

        // TODO start AECM with header arguments
        AuditEncryptionCompressionManager ac =
            AuditEncryptionCompressionManager("zstd", "testKey", "testKeyIdentifier");

        while (getline(input, line)) {
            ++numLine;
            try {
                std::string toDecompress = uassertStatusOK(getCompressedMessage(line));
                inputData.push_back(
                    ac.decompress(ConstDataRange(toDecompress.data(), toDecompress.size())));
            } catch (...) {
                uassert(ErrorCodes::FileStreamFailed,
                        str::stream() << "Failed to to decompress line #" << numLine
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
}

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
