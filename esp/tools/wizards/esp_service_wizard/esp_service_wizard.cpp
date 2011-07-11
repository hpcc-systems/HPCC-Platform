// esp_service_wizard.cpp : Defines the initialization routines for the DLL.
//

#include "stdafx.h"
#include <afxdllx.h>
#include "esp_service_wizard.h"
#include "esp_service_wizardaw.h"

#ifdef _PSEUDO_DEBUG
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

static AFX_EXTENSION_MODULE Esp_service_wizardDLL = { NULL, NULL };

extern "C" int APIENTRY
DllMain(HINSTANCE hInstance, DWORD dwReason, LPVOID lpReserved)
{
    if (dwReason == DLL_PROCESS_ATTACH)
    {
        TRACE0("ESP_SERVICE_WIZARD.AWX Initializing!\n");
        
        // Extension DLL one-time initialization
        AfxInitExtensionModule(Esp_service_wizardDLL, hInstance);

        // Insert this DLL into the resource chain
        new CDynLinkLibrary(Esp_service_wizardDLL);

        // Register this custom AppWizard with MFCAPWZ.DLL
        SetCustomAppWizClass(&espaw);
    }
    else if (dwReason == DLL_PROCESS_DETACH)
    {
        TRACE0("ESP_SERVICE_WIZARD.AWX Terminating!\n");

        // Terminate the library before destructors are called
        AfxTermExtensionModule(Esp_service_wizardDLL);
    }
    return 1;   // ok
}
