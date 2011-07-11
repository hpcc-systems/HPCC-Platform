// main_wiz_dlg.cpp : implementation file
//

#include "stdafx.h"
#include "esp_simple_client.h"
#include "wiz_dlg_main.h"
#include "esp_simple_clientaw.h"
#include "PxShlAPI.h"

#ifdef _PSEUDO_DEBUG
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/////////////////////////////////////////////////////////////////////////////
// CEspMainWizDlg dialog


CEspMainWizDlg::CEspMainWizDlg()
    : CAppWizStepDlg(CEspMainWizDlg::IDD), inited(false)
{
    //{{AFX_DATA_INIT(CEspMainWizDlg)
    m_service = _T("");
    m_abbrev = _T("");
    m_scm_file = _T("");
    m_esp_path = _T("");
    //}}AFX_DATA_INIT
}


void CEspMainWizDlg::DoDataExchange(CDataExchange* pDX)
{
    CAppWizStepDlg::DoDataExchange(pDX);
    //{{AFX_DATA_MAP(CEspMainWizDlg)
    DDX_Text(pDX, IDC_SERVICE_NAME, m_service);
    DDX_Text(pDX, IDC_ABBREV, m_abbrev);
    DDX_Text(pDX, IDC_EXISTING_SCM_FILE, m_scm_file);
    DDX_Text(pDX, IDC_ESP_PATH, m_esp_path);
    //}}AFX_DATA_MAP
}


CString GetRelativePathTo(const char *rootPath, const char *fullPath)
{
    if (rootPath!=NULL && fullPath!=NULL)
    {
        CString relPath;

        if (strnicmp(rootPath, fullPath, strlen(rootPath))==0)
        {
            int count=0;
            int pos=strlen(rootPath);

            for (;fullPath[pos]!=0;pos++)
                if (fullPath[pos]=='/')
                    count++;

            if (count)
            {
                relPath="..";
                while (--count)
                    relPath+="/..";
            }
            else
            {
                relPath=".";
            }
        }
        else
        {
            relPath=rootPath;
        }
            
        return relPath;
    }
    
    return rootPath;
}



// This is called whenever the user presses Next, Back, or Finish with this step
//  present.  Do all validation & data exchange from the dialog in this function.
BOOL CEspMainWizDlg::OnDismiss()
{
    if (!UpdateData(TRUE))
        return FALSE;
    
    if (m_service.IsEmpty())
    {
        MessageBox("Service Name required.");
        return FALSE;
    }

    espaw.m_Dictionary["ESP_SERVICE"]=m_service;

    if (m_abbrev.IsEmpty())
        espaw.m_Dictionary["ESP_ABBREV"]=m_service;
    else
        espaw.m_Dictionary["ESP_ABBREV"]=m_abbrev;

    if (m_scm_file.IsEmpty())
    {
        espaw.m_Dictionary.RemoveKey("ESP_EXISTING_SCM_FILE");
    }
    else
    {
        espaw.m_Dictionary["ESP_SCM_FILE"]=m_scm_file;
        espaw.m_Dictionary["ESP_EXISTING_SCM_FILE"]="TRUE";
    }

    if (m_esp_path.IsEmpty())
    {
        espaw.m_Dictionary.RemoveKey("ESP_PATH");
    }
    else
    {
        CString fullPath(espaw.m_Dictionary["FULL_DIR_PATH"]);

        fullPath.TrimRight(" \\/");
        fullPath.Replace('\\', '/');

        m_esp_path.TrimRight(" \\/");
        m_esp_path.Replace('\\', '/');

        CString relEspPath=GetRelativePathTo(m_esp_path, fullPath);
        
        CString rootPath;
        CString relRootPath;

        if (m_esp_path.Right(4)=="/esp")
        {
            rootPath = m_esp_path.Left(m_esp_path.GetLength()-4);
            if (relEspPath.Left(2)=="..")
                relRootPath = "../" + relEspPath;
            else
                relRootPath=rootPath;
        }

        espaw.m_Dictionary["ESP_PATH"]=m_esp_path;
        espaw.m_Dictionary["ESP_RELATIVE_PATH"]=relEspPath;
        espaw.m_Dictionary["ESP_RELATIVE_ROOT_PATH"]=relRootPath;
    }

    return TRUE;    // return FALSE if the dialog shouldn't be dismissed
}


BEGIN_MESSAGE_MAP(CEspMainWizDlg, CAppWizStepDlg)
    //{{AFX_MSG_MAP(CEspMainWizDlg)
    ON_BN_CLICKED(IDC_BROWSE_SCM_FILE, OnBrowseScmFile)
    ON_BN_CLICKED(IDC_BROWSE_ESP_ROOT, OnBrowseEspRoot)
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()


/////////////////////////////////////////////////////////////////////////////
// CEspMainWizDlg message handlers



void CEspMainWizDlg::OnBrowseScmFile() 
{
    UpdateData();

    CFileDialog scmFileDlg(TRUE, NULL, NULL, OFN_HIDEREADONLY | OFN_OVERWRITEPROMPT, "SCM Files (*.scm)|*.scm||");

    if (scmFileDlg.DoModal()==IDOK)
    {
        m_scm_file=scmFileDlg.GetPathName();
    }
    
    UpdateData(FALSE);
}

void CEspMainWizDlg::OnBrowseEspRoot() 
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
            m_esp_path=browsePath;
    }

    UpdateData(FALSE);
}

BOOL CEspMainWizDlg::OnInitDialog() 
{
    CAppWizStepDlg::OnInitDialog();
    return TRUE;
}


void CEspMainWizDlg::initWizDlg()
{
    if (inited==false)
    {
        inited=true;
        
        m_service = espaw.m_Dictionary["Root"];
        
        m_esp_path = espaw.m_Dictionary["FULL_DIR_PATH"];
        int espos = m_esp_path.Find("\\esp\\", 0);

        if (espos!=-1)
        {
            m_esp_path = m_esp_path.Left(espos+4);
        }
    }
}
