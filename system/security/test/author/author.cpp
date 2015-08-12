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

// author.cpp : Defines the class behaviors for the application.
//

#include "stdafx.h"
#include "author.h"
#include "authorDlg.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/////////////////////////////////////////////////////////////////////////////
// CAuthorApp

BEGIN_MESSAGE_MAP(CAuthorApp, CWinApp)
    //{{AFX_MSG_MAP(CAuthorApp)
    //}}AFX_MSG
    ON_COMMAND(ID_HELP, CWinApp::OnHelp)
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CAuthorApp construction

CAuthorApp::CAuthorApp()
{
}

/////////////////////////////////////////////////////////////////////////////
// The one and only CAuthorApp object

CAuthorApp theApp;

/////////////////////////////////////////////////////////////////////////////
// CAuthorApp initialization

BOOL CAuthorApp::InitInstance()
{
    AfxEnableControlContainer();

    // Standard initialization

    CAuthorDlg dlg;
    m_pMainWnd = &dlg;
    int nResponse = dlg.DoModal();
    if (nResponse == IDOK)
    {
    }
    else if (nResponse == IDCANCEL)
    {
    }

    // Since the dialog has been closed, return FALSE so that we exit the
    //  application, rather than start the application's message pump.
    return FALSE;
}
