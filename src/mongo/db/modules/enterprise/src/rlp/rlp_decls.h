/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <bt_rlp_c.h>
// clang-format off
/**
 * List of functions we depend on from RLP C API
 *
 * These functions are loaded dynamically at runtime from btrlpcorec.so, and
 * sorted by function name.
 *
 * Parameters
 * - Return type
 * - Function Name
 * - Arguments to function
 */
#define RLP_C_FUNC(DECL)                                                                                                                                                                               \
    DECL(const char *, BT_RLP_Library_VersionString, (void))                                                                                                                                           \
                                                                                                                                                                                                       \
    DECL(bool, BT_RLP_Library_VersionIsCompatible, (unsigned long vers))                                                                                                                               \
                                                                                                                                                                                                       \
    DECL(BT_RLP_EnvironmentC *, BT_RLP_Environment_Create, (void))                                                                                                                                     \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_Environment_Destroy, (BT_RLP_EnvironmentC * envp))                                                                                                                               \
                                                                                                                                                                                                       \
    DECL(BT_Result, BT_RLP_Environment_InitializeFromFile, (BT_RLP_EnvironmentC * envp, const char *pathname))                                                                                         \
                                                                                                                                                                                                       \
    DECL(BT_Result, BT_RLP_Environment_GetContextFromBuffer, (BT_RLP_EnvironmentC * envp, const unsigned char *contextspec, BT_UInt32 len, BT_RLP_ContextC **contextpp))                               \
                                                                                                                                                                                                       \
    DECL(bool, BT_RLP_Environment_HasLicenseForLanguage, (const BT_RLP_EnvironmentC *envp, BT_LanguageID lid, BT_UInt32 functionality))                                                                \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_Environment_SetBTRootDirectory, (const char *root_directory_pathname))                                                                                                           \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_Environment_SetLogCallbackFunction, (void *info_p, BT_Log_callback_function fcn_p))                                                                                              \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_Environment_SetLogLevel, (const char *log_level_string))                                                                                                                         \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_Environment_DestroyContext, (BT_RLP_EnvironmentC * envp, BT_RLP_ContextC * contextp))                                                                                            \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_Context_DestroyResultStorage, (BT_RLP_ContextC * contextp))                                                                                                                      \
                                                                                                                                                                                                       \
    DECL(BT_Result, BT_RLP_Context_ProcessBuffer, (BT_RLP_ContextC * contextp, const unsigned char *inbuf, BT_UInt32 inlen, BT_LanguageID lid, const char *character_encoding, const char *mime_type)) \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_Context_SetPropertyValue, (BT_RLP_ContextC * contextp, const char *property_name, const char *property_value))                                                                   \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_TokenIterator_Destroy, (BT_RLP_TokenIteratorC * tkitp))                                                                                                                          \
                                                                                                                                                                                                       \
    DECL(const BT_Char16 *, BT_RLP_TokenIterator_GetLemmaForm, (const BT_RLP_TokenIteratorC *tkitp))                                                                                                   \
                                                                                                                                                                                                       \
    DECL(const BT_Char16 *, BT_RLP_TokenIterator_GetStemForm, (const BT_RLP_TokenIteratorC *tkitp))                                                                                                    \
                                                                                                                                                                                                       \
    DECL(const BT_Char16 *, BT_RLP_TokenIterator_GetToken, (const BT_RLP_TokenIteratorC *tkitp))                                                                                                       \
                                                                                                                                                                                                       \
    DECL(bool, BT_RLP_TokenIterator_IsStopword, (const BT_RLP_TokenIteratorC *tkitp))                                                                                                                  \
                                                                                                                                                                                                       \
    DECL(bool, BT_RLP_TokenIterator_Next, (BT_RLP_TokenIteratorC * tkitp))                                                                                                                             \
                                                                                                                                                                                                       \
    DECL(BT_RLP_TokenIteratorFactoryC *, BT_RLP_TokenIteratorFactory_Create, (void))                                                                                                                   \
                                                                                                                                                                                                       \
    DECL(BT_RLP_TokenIteratorC *, BT_RLP_TokenIteratorFactory_CreateIterator, (BT_RLP_TokenIteratorFactoryC * tkitfacp, const BT_RLP_ContextC *contextp))                                              \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_TokenIteratorFactory_Destroy, (BT_RLP_TokenIteratorFactoryC * tkitfacp))                                                                                                         \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_TokenIteratorFactory_SetReturnCompoundComponents, (BT_RLP_TokenIteratorFactoryC * tkitfacp, bool flag))                                                                          \
                                                                                                                                                                                                       \
    DECL(void, BT_RLP_TokenIteratorFactory_SetReturnReadings, (BT_RLP_TokenIteratorFactoryC * tkitfacp, bool flag))                                                                                    \
                                                                                                                                                                                                       \
    DECL(size_t, bt_xutf16toutf8_lengths, (char *outbuf, size_t outbuf_size, const BT_Char16 *inbuf, size_t inbuf_size, bool *truncated))                                                              \
                                                                                                                                                                                                       \
    DECL(size_t, bt_xwcslen, (const BT_Char16 *s1))

// clang-format on
