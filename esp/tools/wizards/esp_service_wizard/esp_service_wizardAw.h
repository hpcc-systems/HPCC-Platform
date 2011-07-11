#if !defined(AFX_ESP_SERVICE_WIZARDAW_H__FC5B5056_C456_465C_B3BD_5E3F365AF15E__INCLUDED_)
#define AFX_ESP_SERVICE_WIZARDAW_H__FC5B5056_C456_465C_B3BD_5E3F365AF15E__INCLUDED_

// esp_service_wizardaw.h : header file
//

class CDialogChooser;

// All function calls made by mfcapwz.dll to this custom AppWizard (except for
//  GetCustomAppWizClass-- see esp_service_wizard.cpp) are through this class.  You may
//  choose to override more of the CCustomAppWiz virtual functions here to
//  further specialize the behavior of this custom AppWizard.
class CEsp_service_wizardAppWiz : public CCustomAppWiz
{
public:
    virtual CAppWizStepDlg* Next(CAppWizStepDlg* pDlg);
    virtual CAppWizStepDlg* Back(CAppWizStepDlg* pDlg);
        
    virtual void InitCustomAppWiz();
    virtual void ExitCustomAppWiz();
    virtual void CustomizeProject(IBuildProject* pProject);

protected:
    CDialogChooser* m_pChooser;
};

// This declares the one instance of the CEsp_service_wizardAppWiz class.  You can access
//  m_Dictionary and any other public members of this class through the
//  global Esp_service_wizardaw.  (Its definition is in esp_service_wizardaw.cpp.)
extern CEsp_service_wizardAppWiz espaw;

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_ESP_SERVICE_WIZARDAW_H__FC5B5056_C456_465C_B3BD_5E3F365AF15E__INCLUDED_)
