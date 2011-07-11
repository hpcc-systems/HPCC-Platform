#if !defined(AFX_request_wiz_dlg_H__D03A0ACF_F075_4155_8591_12F3BE232E06__INCLUDED_)
#define AFX_request_wiz_dlg_H__D03A0ACF_F075_4155_8591_12F3BE232E06__INCLUDED_

// request_wiz_dlg.h : header file
//

#include "wiz_dlg.h"

/////////////////////////////////////////////////////////////////////////////
// CEspRequestWizDlg dialog

class CEspRequestWizDlg : public CAppWizStepDlg, public IEspWizDlg
{
// Construction
public:
    CEspRequestWizDlg();
    virtual BOOL OnDismiss();

// Dialog Data
    //{{AFX_DATA(CEspRequestWizDlg)
    enum { IDD = IDD_CUSTOM3};
    CListBox    m_parameterList;
    CString m_parameterToAdd;
    int     m_typeToAdd;
    //}}AFX_DATA

    bool inited;
    void initWizDlg()
    {
    }

// Overrides
    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CEspRequestWizDlg)
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL
    
    int m_count;
// Implementation
protected:
    // Generated message map functions
    //{{AFX_MSG(CEspRequestWizDlg)
    afx_msg void OnBtnAddParameter();
    afx_msg void OnBtnDelete();
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()
};


//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_request_wiz_dlg_H__D03A0ACF_F075_4155_8591_12F3BE232E06__INCLUDED_)
