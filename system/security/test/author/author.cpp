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
