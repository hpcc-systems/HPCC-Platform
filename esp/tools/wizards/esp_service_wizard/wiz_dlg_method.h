#if !defined(AFX_defines_wiz_dlg_H__E60CA759_C12E_4C9C_96FA_901C4BC685B4__INCLUDED_)
#define AFX_defines_wiz_dlg_H__E60CA759_C12E_4C9C_96FA_901C4BC685B4__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000
// defines_wiz_dlg.h : header file
//

#include "wiz_dlg.h"

/////////////////////////////////////////////////////////////////////////////
// CEspMethodWizDlg dialog

class CEspMethodWizDlg : public CAppWizStepDlg, public IEspWizDlg
{
// Construction
public:
    CEspMethodWizDlg();   // standard constructor
    virtual BOOL OnDismiss();

// Dialog Data
    //{{AFX_DATA(CEspMethodWizDlg)
    enum { IDD = IDD_CUSTOM2 };
    CString m_method;
    CString m_scm_path;
    //}}AFX_DATA

    bool inited;
    void initWizDlg()
    {
        if (inited==false)
        {
            inited=true;

            m_scm_path = espaw.m_Dictionary["ESP_PATH"];
            m_scm_path+="/";
            m_scm_path+="scm";

            m_method = espaw.m_Dictionary["root"];
        }
    }


// Overrides
    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CEspMethodWizDlg)
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL

// Implementation
protected:

    // Generated message map functions
    //{{AFX_MSG(CEspMethodWizDlg)
    afx_msg void OnBrowseScmFile();
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()
};

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_defines_wiz_dlg_H__E60CA759_C12E_4C9C_96FA_901C4BC685B4__INCLUDED_)
