// esp_simple_client.cpp : Defines the initialization routines for the DLL.
//

#include "stdafx.h"
#include <afxdllx.h>
#include "esp_simple_client.h"
#include "esp_simple_clientaw.h"

#ifdef _PSEUDO_DEBUG
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

static AFX_EXTENSION_MODULE Esp_simple_clientDLL = { NULL, NULL };

extern "C" int APIENTRY
DllMain(HINSTANCE hInstance, DWORD dwReason, LPVOID lpReserved)
{
    if (dwReason == DLL_PROCESS_ATTACH)
    {
        TRACE0("ESP_SIMPLE_CLIENT.AWX Initializing!\n");
        
        // Extension DLL one-time initialization
        AfxInitExtensionModule(Esp_simple_clientDLL, hInstance);

        // Insert this DLL into the resource chain
        new CDynLinkLibrary(Esp_simple_clientDLL);

        // Register this custom AppWizard with MFCAPWZ.DLL
        SetCustomAppWizClass(&espaw);
    }
    else if (dwReason == DLL_PROCESS_DETACH)
    {
        TRACE0("ESP_SIMPLE_CLIENT.AWX Terminating!\n");

        // Terminate the library before destructors are called
        AfxTermExtensionModule(Esp_simple_clientDLL);
    }
    return 1;   // ok
}
