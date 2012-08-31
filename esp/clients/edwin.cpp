/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN     // Exclude rarely-used stuff from Windows headers
#include <windows.h>

#include "edwin.h"

__declspec(thread) HWND gt_hWndThunking = 0;

int g_hThread = 0;

ATOM g_hWinClass = 0;
HANDLE g_hModule = 0;


void *GetThunkingHandle()
{
   //assert(0);
   if (gt_hWndThunking == 0 && g_hWinClass)
   {
      gt_hWndThunking = CreateWindow("edwin","edwin",0,0,0,0,0,0,0, GetModuleHandle(NULL),(LPVOID)4242);
   }
   
   return (void *)gt_hWndThunking;
}

typedef int (*PFN_THUNK_CLIENT)(void *);

void ThunkToClientThread(void *hThunk, PFN_THUNK_CLIENT fn, void *data)
{
   ::PostMessage((HWND)hThunk, (WM_USER + 4242), (WPARAM)fn, (LPARAM) data);
}



LRESULT CALLBACK edwinProc
(
  HWND hwnd,      // handle to window
  UINT uMsg,      // message identifier
  WPARAM wParam,  // first message parameter
  LPARAM lParam   // second message parameter
)
{
   switch (uMsg) 
   { 
      case (WM_USER + 4242):
      {
         if (wParam)
         {
            ((PFN_THUNK_CLIENT)(wParam))((void*)lParam);
         }
         return 0; 
      }
      default: 
         return DefWindowProc(hwnd, uMsg, wParam, lParam); 
    } 
    
   return 0;
}





BOOL APIENTRY DllMain
( 
   HANDLE hModule, 
   DWORD  ul_reason_for_call, 
   LPVOID lpReserved
)
{
   //assert(0);

   switch (ul_reason_for_call)
   {
      case DLL_PROCESS_DETACH:
      {
         UnregisterClass("edwin", GetModuleHandle(NULL));
      }
      
      //fall through

      case DLL_THREAD_DETACH:
//         if (gt_hWndThunking != 0)
  //       {
    //        DestroyWindow(gt_hWndThunking);
      //      gt_hWndThunking = 0;
        // }
         break;

      
      case DLL_PROCESS_ATTACH:
      {
         WNDCLASS edwinClass;
         memset(&edwinClass, 0, sizeof(edwinClass));

         edwinClass.hInstance = GetModuleHandle(NULL);
         edwinClass.lpfnWndProc = edwinProc;
         edwinClass.lpszClassName = "edwin";
      
         g_hWinClass = RegisterClass(&edwinClass);
         g_hModule = hModule;
      }
      
      //fall through

      case DLL_THREAD_ATTACH:
      {
      }
      break;

      default:
         break;
   }

   return TRUE;
}


int GetResourceData(const char *restype, int resid, void *&data, unsigned &len)
{
   HRSRC hRsrc = ::FindResource((HINSTANCE)g_hModule, MAKEINTRESOURCE(resid), restype);

   if (hRsrc != NULL)
   {
      len = ::SizeofResource((HINSTANCE)g_hModule, hRsrc);
      
      if (len > 0)
      {
         HGLOBAL hResData = ::LoadResource((HINSTANCE)g_hModule, hRsrc);

         if (hResData != NULL)
         {
            data = ::LockResource(hResData);
         }
      }
   }
   
   return 0;
}

#endif//_WIN32
