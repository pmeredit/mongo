/**
 *  Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <iostream>
#include <memory>
#include <vector>

#include "decrypt_tool_options.h"

#include "encrypted_data_protector.h"
#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/base/secure_allocator.h"
#include "mongo/base/status_with.h"
#include "mongo/db/service_context.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/text.h"

#include "encryption_key_acquisition.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/crypto/symmetric_key.h"

namespace mongo {
namespace {

int decryptToolMain(int argc, char* argv[]) {
    setupSignalHandlers();
    runGlobalInitializersOrDie(std::vector<std::string>(argv, argv + argc));
    startSignalProcessingThread();

    setGlobalServiceContext(ServiceContext::make());

    StatusWith<std::unique_ptr<SymmetricKey>> swDecryptKey =
        Status(ErrorCodes::InternalError, "Failed to find a valid source of keys");
    if (!globalDecryptToolOptions.keyFile.empty()) {
        swDecryptKey = getKeyFromKeyFile(globalDecryptToolOptions.keyFile.string());
    } else if (!globalDecryptToolOptions.kmipParams.kmipKeyIdentifier.empty() &&
               !globalDecryptToolOptions.kmipParams.kmipServerName.empty()) {
        SSLParams sslParams;
        sslParams.sslFIPSMode = false;
        swDecryptKey = getKeyFromKMIPServer(globalDecryptToolOptions.kmipParams,
                                            globalDecryptToolOptions.kmipParams.kmipKeyIdentifier,
                                            true /* ignore KMIP Key state */);
    } else {
        std::cout
            << "Must have either a keyFile, or a valid KMIP configuration to perform decryption"
            << std::endl;
        return 1;
    }

    if (!swDecryptKey.isOK()) {
        std::cout << "Failed to acquire decryption key: " << swDecryptKey.getStatus().reason()
                  << std::endl;
        return 1;
    }

    if (!boost::filesystem::exists(globalDecryptToolOptions.inputPath)) {
        std::cout << "The file '" << globalDecryptToolOptions.inputPath << "' could not be found."
                  << std::endl;
        return 1;
    }
    if (!boost::filesystem::is_regular_file(globalDecryptToolOptions.inputPath)) {
        std::cout << "The file '" << globalDecryptToolOptions.inputPath
                  << "' was not a regular file." << std::endl;
        return 1;
    }
    if (boost::filesystem::exists(globalDecryptToolOptions.outputPath)) {
        std::cout << "'" << globalDecryptToolOptions.outputPath
                  << "' already exists. Refusing to overwrite." << std::endl;
        return 1;
    }

    if (globalDecryptToolOptions.confirmDecryption) {
        std::cout
            << "The following file is about to be decrypted: " << globalDecryptToolOptions.inputPath
            << std::endl
            << "The decrypted contents will be placed in: " << globalDecryptToolOptions.outputPath
            << std::endl
            << "Please be aware that this operation may potentially place sensitive information "
            << "in clear text on your filesystem." << std::endl
            << std::endl
            << "Do you wish to continue? (Yes/No) [Default: No] ";
        std::string userConfirmation;
        std::getline(std::cin, userConfirmation);
        if (userConfirmation != "yes" && userConfirmation != "Yes") {
            std::cout << "Not continuing." << std::endl;
            return 1;
        }
    }

    // Open files
    boost::filesystem::ifstream input(globalDecryptToolOptions.inputPath,
                                      std::ios_base::in | std::ios_base::binary);
    if (!input.is_open()) {
        std::cout << "Failed to open encrypted file '" << globalDecryptToolOptions.inputPath << "'"
                  << std::endl;
        return 1;
    }
    boost::filesystem::ofstream output(globalDecryptToolOptions.outputPath.c_str(),
                                       std::ios_base::out | std::ios_base::binary);
    if (!output.is_open()) {
        std::cout << "Failed to open output file '" << globalDecryptToolOptions.outputPath << "'"
                  << std::endl;
        return 1;
    }

    // Read in data
    std::vector<uint8_t> inputData;
    std::copy(std::istreambuf_iterator<char>(input),
              std::istreambuf_iterator<char>(),
              std::back_inserter(inputData));
    if (inputData.empty()) {
        std::cout << "Read no data." << std::endl;
        return 1;
    }

    if (inputData[0] != DATA_PROTECTOR_VERSION_0) {
        std::cout << "Got version number '" << inputData[0] << "' but expected '0'" << std::endl;
        return -1;
    }

    std::vector<uint8_t> outputData(inputData.size() - 1);

    size_t resultLen;
    // Encrypted Data Protector's protect() method writes out this file using a
    // format which is equivalent to having called aesEncrypt with PageSchema::k0,
    // but in a streaming mode.
    Status ret = aesDecrypt(*swDecryptKey.getValue(),
                            globalDecryptToolOptions.mode,
                            crypto::PageSchema::k0,
                            inputData.data() + 1,
                            inputData.size() - 1,
                            outputData.data(),
                            outputData.size(),
                            &resultLen);
    if (!ret.isOK()) {
        std::cout << "Failed to decrypt encrypted file '" << globalDecryptToolOptions.inputPath
                  << "': " << ret.reason() << std::endl;
        return 1;
    }
    outputData.resize(resultLen);

    // Write out cleartext
    std::copy(outputData.begin(), outputData.end(), std::ostreambuf_iterator<char>(output));

    std::cout << "Successfully wrote " << outputData.size() << " bytes to '"
              << globalDecryptToolOptions.outputPath << "'." << std::endl;

    return 0;
}
}  // namespace
}  // namespace mongo

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through argv().  This enables decryptToolMain()
// to process UTF-8 encoded arguments without regard to platform.
int wmain(int argc, wchar_t** argvW) {
    mongo::WindowsCommandLine wcl(argc, argvW);
    int exitCode = mongo::decryptToolMain(argc, wcl.argv());
    mongo::quickExit(exitCode);
}
#else
int main(int argc, char** argv) {
    int exitCode = mongo::decryptToolMain(argc, argv);
    mongo::quickExit(exitCode);
}
#endif
