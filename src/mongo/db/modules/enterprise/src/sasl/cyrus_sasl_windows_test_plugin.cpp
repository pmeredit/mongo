/**
 *  Copyright (C) 2024 MongoDB Inc.
 */

#include <stdio.h>

BOOL WINAPI DllMain(HINSTANCE hinstDLL,  // handle to DLL module
                    DWORD fdwReason,     // reason for calling function
                    LPVOID lpReserved)   // reserved
{
    printf("Loaded cyrus_sasl_windows_test_plugin.dll\n");
    return TRUE;
}
