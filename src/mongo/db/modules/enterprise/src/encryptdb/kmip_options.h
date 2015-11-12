/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <string>

namespace mongo {
class Status;

namespace optionenvironment {
class OptionSection;
class Environment;
}  // namespace optionenvironment

namespace moe = mongo::optionenvironment;

struct KMIPParams {
    int kmipPort = 5696;
    std::string kmipKeyIdentifier;
    std::string kmipServerName;
    std::string kmipClientCertificateFile;
    std::string kmipClientCertificatePassword;
    std::string kmipServerCAFile;
};

void addKMIPOptions(moe::OptionSection* options);
KMIPParams parseKMIPOptions(const moe::Environment& params);

}  // namespace mongo
