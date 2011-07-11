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
