#if !defined(AFX_ESP_SIMPLE_CLIENTAW_H__D733D3C9_819E_42AA_B385_94008ADF289F__INCLUDED_)
#define AFX_ESP_SIMPLE_CLIENTAW_H__D733D3C9_819E_42AA_B385_94008ADF289F__INCLUDED_

// esp_simple_clientaw.h : header file
//

class CDialogChooser;

// All function calls made by mfcapwz.dll to this custom AppWizard (except for
//  GetCustomAppWizClass-- see esp_simple_client.cpp) are through this class.  You may
//  choose to override more of the CCustomAppWiz virtual functions here to
//  further specialize the behavior of this custom AppWizard.
class CEsp_simple_clientAppWiz : public CCustomAppWiz
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

// This declares the one instance of the CEsp_simple_clientAppWiz class.  You can access
//  m_Dictionary and any other public members of this class through the
//  global Esp_simple_clientaw.  (Its definition is in esp_simple_clientaw.cpp.)
extern CEsp_simple_clientAppWiz espaw;

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_ESP_SIMPLE_CLIENTAW_H__D733D3C9_819E_42AA_B385_94008ADF289F__INCLUDED_)
