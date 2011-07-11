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

#if !defined(AFX_NEWVALUEDLG_H__8B38AC7E_2682_4489_8AE0_1561A7206360__INCLUDED_)
#define AFX_NEWVALUEDLG_H__8B38AC7E_2682_4489_8AE0_1561A7206360__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000
// newvaluedlg.h : header file
//

#include "resource.h"

enum NewValue_t { NVT_property, NVT_attribute };


/////////////////////////////////////////////////////////////////////////////
// CNewValueDlg dialog

class CNewValueDlg : public CDialog
{
// Construction
public:
    LPCSTR GetValue();
    LPCSTR GetName();
    CNewValueDlg(NewValue_t nvt, LPCSTR initName, LPCSTR initValue, CWnd* pParent = NULL);   // standard constructor

// Dialog Data
    //{{AFX_DATA(CNewValueDlg)
    enum { IDD = IDD_NEWVALUE };
        // NOTE: the ClassWizard will add data members here
    //}}AFX_DATA


// Overrides
    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CNewValueDlg)
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL

// Implementation
protected:
    bool Validate();

    // Generated message map functions
    //{{AFX_MSG(CNewValueDlg)
    virtual void OnOK();
    virtual BOOL OnInitDialog();
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()
private:
    NewValue_t NewValueType;
    CString Value;
    CString Name;
};

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_NEWVALUEDLG_H__8B38AC7E_2682_4489_8AE0_1561A7206360__INCLUDED_)
