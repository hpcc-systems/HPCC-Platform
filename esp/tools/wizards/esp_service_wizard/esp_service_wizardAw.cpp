// esp_service_wizardaw.cpp : implementation file
//

#include "stdafx.h"
#include "esp_service_wizard.h"
#include "esp_service_wizardaw.h"
#include "chooser.h"

#ifdef _PSEUDO_DEBUG
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

// This is called immediately after the custom AppWizard is loaded.  Initialize
//  the state of the custom AppWizard here.
void CEsp_service_wizardAppWiz::InitCustomAppWiz()
{
    // Create a new dialog chooser; CDialogChooser's constructor initializes
    //  its internal array with pointers to the steps.
    m_pChooser = new CDialogChooser;
    
    // There are no steps in this custom AppWizard.
    SetNumberOfSteps(LAST_DLG);

    // Add build step to .hpj if there is one
    m_Dictionary[_T("HELP")] = _T("1");

    // Inform AppWizard that we're making a DLL.
    m_Dictionary[_T("PROJTYPE_DLL")] = _T("1");

    // Set template macros based on the project name entered by the user.
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
void CEsp_service_wizardAppWiz::ExitCustomAppWiz()
{
    // Deallocate memory used for the dialog chooser
    ASSERT(m_pChooser != NULL);
    delete m_pChooser;
    m_pChooser = NULL;
}

// This is called when the user clicks "Create..." on the New Project dialog
CAppWizStepDlg* CEsp_service_wizardAppWiz::Next(CAppWizStepDlg* pDlg)
{
    // Delegate to the dialog chooser
    return m_pChooser->Next(pDlg);
}

// This is called when the user clicks "Back" on one of the custom
//  AppWizard's steps.
CAppWizStepDlg* CEsp_service_wizardAppWiz::Back(CAppWizStepDlg* pDlg)
{
    // Delegate to the dialog chooser
    return m_pChooser->Back(pDlg);
}



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

void CEsp_service_wizardAppWiz::CustomizeProject(IBuildProject* pProject)
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
    CString espFile;
    CString hppFile;

    //build path to the SCM file
    if (scmFile.IsEmpty())
    {
        if (!scmPath.IsEmpty())
            scmFile = scmPath + "/";
        scmFile += m_Dictionary["root"] + ".scm";
    }

    //build path to what will be the generated esp file
    if (!scmPath.IsEmpty())
    {
        espFile = scmPath + "/";
        hppFile = scmPath + "/";
    }

    espFile += m_Dictionary["root"] + ".esp";
    hppFile += m_Dictionary["root"] + ".hpp";

    pProject->AddFile(aBSTR(scmFile), resVar);
    pProject->AddFile(aBSTR(espFile), resVar);
    pProject->AddFile(aBSTR(hppFile), resVar);

    CString strAddFilePath;
    strAddFilePath.Format("%s\\bin\\release\\jlib.lib", (LPCSTR)relBsRootPath); 
    pProject->AddFile(aBSTR(strAddFilePath), resVar);

    strAddFilePath.Format("%s\\bin\\release\\ldapSecurity.lib", (LPCSTR)relBsRootPath); 
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

                strSetting.Format("%s\\..\\bin\\%s\\hidl -esp $(InputPath)", (LPCSTR)relBsPath, (LPCSTR)strBuild); 
                CString strOutputs = hppFile + "\n" + espFile;
                pConfig->AddCustomBuildStepToFile(aBSTR(scmFile), aBSTR(strSetting), aBSTR(strOutputs), aBSTR(""), resVar);

                pConfig->Release();
            }
        }
    
        pConfigs->Release();
    }
}


// Here we define one instance of the CEsp_service_wizardAppWiz class.  You can access
//  m_Dictionary and any other public members of this class through the
//  global Esp_service_wizardaw.
CEsp_service_wizardAppWiz espaw;

