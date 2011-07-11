// esp_simple_clientaw.cpp : implementation file
//

#include "stdafx.h"
#include "esp_simple_client.h"
#include "esp_simple_clientaw.h"
#include "chooser.h"

#ifdef _PSEUDO_DEBUG
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif


class aBSTR
{
    BSTR val_;
    bool isDirty;
public:
    
    aBSTR(BSTR val=0) : val_(val), isDirty(false) {}
    aBSTR(CString &val) : isDirty(false) {val_=val.AllocSysString();}
    aBSTR(LPCSTR val) : isDirty(false) 
    {
        if (val)
        {
            int nLen = ::MultiByteToWideChar(CP_ACP, 0, val, strlen(val), NULL, NULL);
            val_ = ::SysAllocStringLen(NULL, nLen);
            if (val_!=NULL)
            {
                MultiByteToWideChar(CP_ACP, 0, val, strlen(val), val_, nLen);
            }
        }
    }

    ~aBSTR(){::SysFreeString(val_);}

    operator BSTR(){return val_;}
    BSTR *bstrp(){isDirty=true; return &val_;}
};



// This is called immediately after the custom AppWizard is loaded.  Initialize
//  the state of the custom AppWizard here.
void CEsp_simple_clientAppWiz::InitCustomAppWiz()
{
    m_pChooser = new CDialogChooser;
    
    // There are no steps in this custom AppWizard.
    SetNumberOfSteps(1);

    // Add build step to .hpj if there is one
    m_Dictionary[_T("HELP")] = _T("1");

    // Inform AppWizard that we're making a DLL.
    m_Dictionary[_T("PROJTYPE_DLL")] = _T("1");

    // Get value of $$root$$ (already set by AppWizard)
    CString strRoot;
    m_Dictionary.Lookup(_T("root"), strRoot);
    
    // Set value of $$Doc$$, $$DOC$$
    CString strDoc = strRoot.Left(6);
    m_Dictionary[_T("Doc")] = strDoc;
    strDoc.MakeUpper();
    m_Dictionary[_T("DOC")] = strDoc;

    // Set value of $$MAC_TYPE$$
    strRoot = strRoot.Left(4);
    int nLen = strRoot.GetLength();
    if (strRoot.GetLength() < 4)
    {
        CString strPad(_T(' '), 4 - nLen);
        strRoot += strPad;
    }
    strRoot.MakeUpper();
    m_Dictionary[_T("MAC_TYPE")] = strRoot;
}

// This is called just before the custom AppWizard is unloaded.
void CEsp_simple_clientAppWiz::ExitCustomAppWiz()
{
    // Deallocate memory used for the dialog chooser
    ASSERT(m_pChooser != NULL);
    delete m_pChooser;
    m_pChooser = NULL;
}

// This is called when the user clicks "Create..." on the New Project dialog
CAppWizStepDlg* CEsp_simple_clientAppWiz::Next(CAppWizStepDlg* pDlg)
{
    // Return NULL to indicate there are no more steps.  (In this case, there are
    //  no steps at all.)
    return m_pChooser->Next(pDlg);
}

CAppWizStepDlg* CEsp_simple_clientAppWiz::Back(CAppWizStepDlg* pDlg)
{
    // Return NULL to indicate there are no more steps.  (In this case, there are
    //  no steps at all.)
    return m_pChooser->Back(pDlg);
}


void CEsp_simple_clientAppWiz::CustomizeProject(IBuildProject* pProject)
{
    COleVariant resVar;
    COleVariant paramVar;
    
    CString projName = m_Dictionary["root"]; 

    CString relPath = m_Dictionary["ESP_RELATIVE_PATH"];
    CString relBsPath = m_Dictionary["ESP_RELATIVE_PATH"];
    relBsPath.Replace('/','\\');

    CString relRootPath = m_Dictionary["ESP_RELATIVE_ROOT_PATH"];
    CString relBsRootPath = m_Dictionary["ESP_RELATIVE_ROOT_PATH"];
    relBsRootPath.Replace('/','\\');
    
    CString scmPath = m_Dictionary["ESP_SCM_PATH"];
    CString scmFile = m_Dictionary["ESP_SCM_FILE"];

    if (!scmFile.IsEmpty())
        pProject->AddFile(aBSTR(scmFile), resVar);

    CString strAddFilePath;
    strAddFilePath.Format("%s\\esp\\clients\\edwin.cpp", (LPCSTR)relBsRootPath); 
    pProject->AddFile(aBSTR(strAddFilePath), resVar);

    strAddFilePath.Format("%s\\bin\\release\\jlib.lib", (LPCSTR)relBsRootPath); 
    pProject->AddFile(aBSTR(strAddFilePath), resVar);

    strAddFilePath.Format("%s\\bin\\release\\ldap.lib", (LPCSTR)relBsRootPath); 
    pProject->AddFile(aBSTR(strAddFilePath), resVar);

    strAddFilePath.Format("%s\\bin\\release\\commonsecurity.lib", (LPCSTR)relBsRootPath); 
    pProject->AddFile(aBSTR(strAddFilePath), resVar);

    strAddFilePath.Format("%s\\bin\\release\\soaplib.lib", (LPCSTR)relBsRootPath); 
    pProject->AddFile(aBSTR(strAddFilePath), resVar);

    strAddFilePath.Format("%s\\system\\openssl\\out32dll\\ssleay32.lib", (LPCSTR)relBsRootPath); 
    pProject->AddFile(aBSTR(strAddFilePath), resVar);

    strAddFilePath.Format("%s\\system\\openssl\\out32dll\\libeay32.lib", (LPCSTR)relBsRootPath); 
    pProject->AddFile(aBSTR(strAddFilePath), resVar);
    
    
    IConfigurations *pConfigs = NULL;
    pProject->get_Configurations(&pConfigs);

    if (pConfigs!=NULL)
    {
        long ccount=0;
        pConfigs->get_Count(&ccount);
    
        aBSTR clEXE("cl.exe");
        aBSTR linkEXE("link.exe");

        memset(&resVar, 0, sizeof(resVar));

        for (; ccount>0;ccount--)
        {
            IConfiguration *pConfig = NULL;
            pConfigs->Item(COleVariant(ccount), &pConfig);

            if (pConfig!=NULL)
            {
                pConfig->AddToolSettings(linkEXE, aBSTR("wsock32.lib"), resVar);
                pConfig->AddToolSettings(linkEXE, aBSTR("user32.lib"), resVar);
                pConfig->AddToolSettings(clEXE, aBSTR("/GR"), resVar);

                aBSTR bstrCfg;
                pConfig->get_Name(bstrCfg.bstrp());
                _wcslwr( bstrCfg );
                bool isDebug = (wcsstr(bstrCfg , L"debug")!=NULL);

                CString strBuild = isDebug ? "debug" : "release";

                
                CString strSetting;

                strSetting.Format("/pdb:\"%s\\bin\\%s/%s.pdb\"", (LPCSTR)relBsRootPath, (LPCSTR)strBuild, (LPCSTR)projName); 
                pConfig->AddToolSettings(linkEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/map:\"%s\\bin\\%s\\maps\\%s.map\"", (LPCSTR)relBsRootPath, (LPCSTR)strBuild, (LPCSTR)projName); 
                pConfig->AddToolSettings(linkEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/out:\"%s\\bin\\%s/%s.dll\"", (LPCSTR)relBsRootPath, (LPCSTR)strBuild, (LPCSTR)projName); 
                pConfig->AddToolSettings(linkEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/implib:\"%s\\obj\\%s/%s.lib\"",     (LPCSTR)relBsRootPath, (LPCSTR)strBuild, (LPCSTR)projName); 
                pConfig->AddToolSettings(linkEXE, aBSTR(strSetting), resVar);

                pConfig->AddToolSettings(clEXE, aBSTR((isDebug) ? "/MDd" : "/MD"), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, ""); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "../system/security/securesocket"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "../system/security/scm"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "../system/openssl/inc32"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "clients"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "bindings"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "services"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "bindings/soap/xpp"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "scm"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "platform"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "../system/jlib"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "../system/scm"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                strSetting.Format("/I \"%s/%s\"", (LPCSTR)relPath, "../system/include"); 
                pConfig->AddToolSettings(clEXE, aBSTR(strSetting), resVar);

                pConfig->Release();
            }
        }
    
        pConfigs->Release();
    }
}


// Here we define one instance of the CEsp_simple_clientAppWiz class.  You can access
//  m_Dictionary and any other public members of this class through the
//  global Esp_simple_clientaw.
CEsp_simple_clientAppWiz espaw;

