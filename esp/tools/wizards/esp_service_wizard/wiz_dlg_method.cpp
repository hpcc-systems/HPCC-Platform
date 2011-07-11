// defines_wiz_dlg.cpp : implementation file
//

#include "stdafx.h"
#include "esp_service_wizard.h"
#include "esp_service_wizardAw.h"
#include "wiz_dlg_method.h"
#include "PxShlAPI.h"



#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif



/////////////////////////////////////////////////////////////////////////////
// CEspMethodWizDlg dialog


CEspMethodWizDlg::CEspMethodWizDlg()
    : CAppWizStepDlg(CEspMethodWizDlg::IDD), inited(false)
{
    //{{AFX_DATA_INIT(CEspMethodWizDlg)
    m_method = _T("");
    m_scm_path = _T("");
    //}}AFX_DATA_INIT
}

void CEspMethodWizDlg::DoDataExchange(CDataExchange* pDX)
{
    CDialog::DoDataExchange(pDX);
    //{{AFX_DATA_MAP(CEspMethodWizDlg)
    DDX_Text(pDX, IDC_METHOD, m_method);
    DDX_Text(pDX, IDC_SCM_PATH, m_scm_path);
    //}}AFX_DATA_MAP
}


BEGIN_MESSAGE_MAP(CEspMethodWizDlg, CDialog)
    //{{AFX_MSG_MAP(CEspMethodWizDlg)
    ON_BN_CLICKED(IDC_BROWSE_SCM_FILE, OnBrowseScmFile)
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CEspMethodWizDlg message handlers


BOOL CEspMethodWizDlg::OnDismiss()
{
    if (!UpdateData(TRUE))
        return FALSE;
    
    if (m_method.IsEmpty())
    {
        MessageBox("Method Name required.");
        return FALSE;
    }

    if (m_scm_path.IsEmpty())
    {
        MessageBox("SCM Path required.");
        return FALSE;
    }

    espaw.m_Dictionary["ESP_METHOD"]=m_method;
    espaw.m_Dictionary["ESP_SCM_PATH"]=m_scm_path;
    
    return TRUE;
}

void CEspMethodWizDlg::OnBrowseScmFile() 
{
    UpdateData();

    CPidl id_root_path(CSIDL_DRIVES);
    
    char browsePath[MAX_PATH]={0};
    
    BROWSEINFO info;
    memset(&info, 0, sizeof(info));
    
    info.hwndOwner = (HWND) *this;
    info.pszDisplayName=browsePath;
    info.lpszTitle="SCM File Path";
    info.ulFlags = (BIF_BROWSEINCLUDEFILES | BIF_RETURNONLYFSDIRS | BIF_EDITBOX | BIF_RETURNFSANCESTORS);
    info.pidlRoot = id_root_path;

    LPCITEMIDLIST id_list = SHBrowseForFolder(&info);
    if (id_list!=NULL)
    {
        if (SHGetPathFromIDList(id_list, browsePath))
            m_scm_path=browsePath;
    }

    UpdateData(FALSE);
}
