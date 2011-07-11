#if !defined(AFX_main_wiz_dlg_H__C4D8123E_2C64_4B27_A067_436A40AEAEA6__INCLUDED_)
#define AFX_main_wiz_dlg_H__C4D8123E_2C64_4B27_A067_436A40AEAEA6__INCLUDED_

// main_wiz_dlg.h : header file
//

#include "wiz_dlg.h"


/////////////////////////////////////////////////////////////////////////////
// CEspMainWizDlg dialog

class CEspMainWizDlg : public CAppWizStepDlg, public IEspWizDlg
{
// Construction
public:
    CEspMainWizDlg();
    virtual BOOL OnDismiss();

// Dialog Data
    //{{AFX_DATA(CEspMainWizDlg)
    enum { IDD = IDD_CUSTOM1 };
    CString m_service;
    CString m_abbrev;
    CString m_scm_file;
    CString m_esp_path;
    //}}AFX_DATA

    bool inited;
    void initWizDlg();


// Overrides
    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CEspMainWizDlg)
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL

// Implementation
protected:
    // Generated message map functions
    //{{AFX_MSG(CEspMainWizDlg)
    afx_msg void OnBrowseScmFile();
    afx_msg void OnBrowseEspRoot();
    virtual BOOL OnInitDialog();
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()
};


//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_main_wiz_dlg_H__C4D8123E_2C64_4B27_A067_436A40AEAEA6__INCLUDED_)
