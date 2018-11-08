/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

// treeview.cpp : Defines the class behaviors for the application.
//

#include "stdafx.h"
#include "treeview.h"
#include "treeviewDlg.h"
#include "inspectctrl.hpp"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/////////////////////////////////////////////////////////////////////////////
// CTreeviewApp

BEGIN_MESSAGE_MAP(CTreeviewApp, CWinApp)
    //{{AFX_MSG_MAP(CTreeviewApp)
        // NOTE - the ClassWizard will add and remove mapping macros here.
        //    DO NOT EDIT what you see in these blocks of generated code!
    //}}AFX_MSG
    ON_COMMAND(ID_HELP, CWinApp::OnHelp)
END_MESSAGE_MAP()




void CCmdLineInfo::ParseParam(LPCTSTR param, BOOL flag, BOOL last)
{
    if(flag)
    {

    }
    else
    {
        app.inFilename = strdup(param);
    }
}




/////////////////////////////////////////////////////////////////////////////
// CTreeviewApp construction

CTreeviewApp::CTreeviewApp()
{
    cmdLineInfo = new CCmdLineInfo(*this);
    inFilename = NULL;
}

CTreeviewApp::~CTreeviewApp()
{
    free(inFilename);
    delete cmdLineInfo;
}


/////////////////////////////////////////////////////////////////////////////
// The one and only CTreeviewApp object

CTreeviewApp theApp;


/////////////////////////////////////////////////////////////////////////////
// CTreeviewApp initialization
BOOL CTreeviewApp::InitInstance()
{
    // Standard initialization
    // If you are not using these features and wish to reduce the size
    //  of your final executable, you should remove from the following
    //  the specific initialization routines you do not need.

    if(!CWinApp::InitInstance()) return FALSE;

    ParseCommandLine(*cmdLineInfo);

    CPropertyInspector::registerClass();

#ifdef _AFXDLL
    Enable3dControls();         // Call this when using MFC in a shared DLL
#else
    Enable3dControlsStatic();   // Call this when linking to MFC statically
#endif

    CTreeviewDlg dlg(inFilename);
    m_pMainWnd = &dlg;
    INT_PTR nResponse = dlg.DoModal();
    if (nResponse == IDOK)
    {

    }
    else if (nResponse == IDCANCEL)
    {

    }
    return TRUE;
}
