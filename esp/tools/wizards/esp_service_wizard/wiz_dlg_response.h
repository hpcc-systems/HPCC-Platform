#if !defined(AFX_response_wiz_dlg_H__27FF4D8B_3688_417A_B59D_55FB0712ABDD__INCLUDED_)
#define AFX_response_wiz_dlg_H__27FF4D8B_3688_417A_B59D_55FB0712ABDD__INCLUDED_

// response_wiz_dlg.h : header file
//


#include "wiz_dlg.h"


/////////////////////////////////////////////////////////////////////////////
// CEspResponseWizDlg dialog

class CEspResponseWizDlg : public CAppWizStepDlg, public IEspWizDlg
{
// Construction
public:
    CEspResponseWizDlg();
    virtual BOOL OnDismiss();

// Dialog Data
    //{{AFX_DATA(CEspResponseWizDlg)
    enum { IDD = IDD_CUSTOM4 };
    CListBox    m_parameterList;
    CString m_parameterToAdd;
    int     m_typeToAdd;
    //}}AFX_DATA

    int m_count;
    
    bool inited;
    void initWizDlg()
    {
    }

// Overrides
    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CEspResponseWizDlg)
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL

// Implementation
protected:
    // Generated message map functions
    //{{AFX_MSG(CEspResponseWizDlg)
    afx_msg void OnBtnAddParameter();
    afx_msg void OnBtnDelete();
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()
};


//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_response_wiz_dlg_H__27FF4D8B_3688_417A_B59D_55FB0712ABDD__INCLUDED_)
