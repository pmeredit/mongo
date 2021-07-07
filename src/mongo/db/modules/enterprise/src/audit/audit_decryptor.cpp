/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "audit_decryptor_options.h"
#include "audit_enc_comp_manager.h"

#include "mongo/db/service_context.h"
#include "mongo/util/base64.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/text.h"

namespace mongo {
namespace audit {

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
    std::vector<std::string> inputData;
    int numLine = 0;

    AuditEncryptionCompressionManager ac;

    // Read file
    if (input.is_open()) {
        while (getline(input, line)) {
            ++numLine;
            try {
                std::string toWriteDecompressed = ac.decompress(line);
                inputData.push_back(toWriteDecompressed);
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

    std::ofstream output(globalAuditDecryptorOptions.outputPath.c_str(), std::ios::out);

    uassert(ErrorCodes::FileOpenFailed,
            str::stream() << "Failed to open output file '"
                          << globalAuditDecryptorOptions.outputPath.string() << "'",
            output.is_open());

    uassert(ErrorCodes::FileStreamFailed, "Read no data.", !inputData.empty());

    // Write out cleartext
    std::copy(inputData.begin(), inputData.end(), std::ostream_iterator<std::string>(output));

    std::cout << "Successfully wrote " << inputData.size() << " bytes to '"
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
