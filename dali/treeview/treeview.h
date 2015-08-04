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

// treeview.h : main header file for the TREEVIEW application
//

#if !defined(AFX_TREEVIEW_H__B2A09705_710F_4AF0_995C_963FD094A139__INCLUDED_)
#define AFX_TREEVIEW_H__B2A09705_710F_4AF0_995C_963FD094A139__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#ifndef __AFXWIN_H__
    #error include 'stdafx.h' before including this file for PCH
#endif

#include "resource.h"       // main symbols


class CTreeviewApp;         // forward reference


class CCmdLineInfo : public CCommandLineInfo
{
private:
    CTreeviewApp & app;

public:
    CCmdLineInfo(CTreeviewApp & _app) : app(_app) {}

    virtual void ParseParam(LPCTSTR param, BOOL flags, BOOL last);
};


/////////////////////////////////////////////////////////////////////////////
// CTreeviewApp:
// See treeview.cpp for the implementation of this class
//

class CTreeviewApp : public CWinApp
{
private:
    CCmdLineInfo * cmdLineInfo;
    LPSTR inFilename;               // the filename for the tree

public:
    CTreeviewApp();
    ~CTreeviewApp();

// Overrides
    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CTreeviewApp)
    public:
    virtual BOOL InitInstance();
    //}}AFX_VIRTUAL

// Implementation

    //{{AFX_MSG(CTreeviewApp)
        // NOTE - the ClassWizard will add and remove member functions here.
        //    DO NOT EDIT what you see in these blocks of generated code !
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()

friend class CCmdLineInfo;
};


/////////////////////////////////////////////////////////////////////////////

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_TREEVIEW_H__B2A09705_710F_4AF0_995C_963FD094A139__INCLUDED_)
