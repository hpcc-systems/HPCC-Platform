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
