// chooser.cpp : Implements the CDialogChooser class
//

#include "stdafx.h"
#include "esp_simple_client.h"
#include "esp_simple_clientAw.h"
#include "chooser.h"
#include "wiz_dlg.h"
#include "wiz_dlg_main.h"


#ifdef _PSEUDO_DEBUG
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

// On construction, set up internal array with pointers to each step.
CDialogChooser::CDialogChooser()
{
    m_pDlgs[0] = NULL;

    m_pDlgs[1] = new CEspMainWizDlg;

    m_nCurrDlg = 0;
}
// Remember where the custom steps begin, so we can delete them in
//  the destructor
#define FIRST_CUSTOM_STEP 1
#define LAST_CUSTOM_STEP 1

// The destructor deletes entries in the internal array corresponding to
//  custom steps.
CDialogChooser::~CDialogChooser()
{
    for (int i = FIRST_CUSTOM_STEP; i <= LAST_CUSTOM_STEP; i++)
    {
        ASSERT(m_pDlgs[i] != NULL);
        delete m_pDlgs[i];
    }
}

// Use the internal array to determine the next step.
CAppWizStepDlg* CDialogChooser::Next(CAppWizStepDlg* pDlg)
{
    ASSERT(0 <= m_nCurrDlg && m_nCurrDlg < LAST_DLG);
    ASSERT(pDlg == m_pDlgs[m_nCurrDlg]);

    //Rules:
    if (m_nCurrDlg==0)
    {
        m_nCurrDlg=1;

        IEspWizDlg *wd=dynamic_cast<IEspWizDlg*>(m_pDlgs[m_nCurrDlg]);
        wd->initWizDlg();
    }

    return m_pDlgs[m_nCurrDlg];
}

// Use the internal array to determine the previous step.
CAppWizStepDlg* CDialogChooser::Back(CAppWizStepDlg* pDlg)
{
    ASSERT(1 <= m_nCurrDlg && m_nCurrDlg <= LAST_DLG);
    ASSERT(pDlg == m_pDlgs[m_nCurrDlg]);

    if (m_nCurrDlg>0)
        m_nCurrDlg--;

    return m_pDlgs[m_nCurrDlg];
}
