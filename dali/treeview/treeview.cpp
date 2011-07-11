/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    int nResponse = dlg.DoModal();
    if (nResponse == IDOK)
    {

    }
    else if (nResponse == IDCANCEL)
    {

    }
    return TRUE;
}
