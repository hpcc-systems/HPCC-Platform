// request_wiz_dlg.cpp : implementation file
//

#include "stdafx.h"
#include "esp_service_wizard.h"
#include "wiz_dlg_request.h"
#include "esp_service_wizardaw.h"

#ifdef _PSEUDO_DEBUG
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif


/////////////////////////////////////////////////////////////////////////////
// CEspRequestWizDlg dialog


CEspRequestWizDlg::CEspRequestWizDlg()
    : CAppWizStepDlg(CEspRequestWizDlg::IDD), inited(false)
{
    //{{AFX_DATA_INIT(CEspRequestWizDlg)
    m_parameterToAdd = _T("");
    m_typeToAdd = 2;
    //}}AFX_DATA_INIT
    
    m_count=0;
}


void CEspRequestWizDlg::DoDataExchange(CDataExchange* pDX)
{
    CAppWizStepDlg::DoDataExchange(pDX);
    //{{AFX_DATA_MAP(CEspRequestWizDlg)
    DDX_Control(pDX, IDC_PARAMETER_LIST, m_parameterList);
    DDX_Text(pDX, IDC_PARAMETER, m_parameterToAdd);
    DDX_CBIndex(pDX, IDC_PARAMETER_TYPE, m_typeToAdd);
    //}}AFX_DATA_MAP
}



static char *param_types[] = {"integer", "nonNegativeInteger", "string", "boolean"};

static char *param_type_map(const char *type)
{
    if (type!=NULL)
    {
        switch (*type)
        {
        case 'i':
            return "integer";
        case 'n':
            return "nonNegativeInteger";
        case 's':
            return "string";
        case 'b':
            return "boolean";
        default:
            break;
        }
    }
    return "string";
}

// This is called whenever the user presses Next, Back, or Finish with this step
//  present.  Do all validation & data exchange from the dialog in this function.
BOOL CEspRequestWizDlg::OnDismiss()
{
    if (!UpdateData(TRUE))
        return FALSE;

    CString strKey;

    while (--m_count>=0)
    {
        strKey.Format("ESP_REQ_PARM_%d", m_count);
        espaw.m_Dictionary.RemoveKey(strKey);
        strKey.Format("ESP_REQ_PARM_TYPE_%d", m_count);
        espaw.m_Dictionary.RemoveKey(strKey);
    }
    
    char strVal[512];
    int len;
    
    int count = m_count = m_parameterList.GetCount();
    espaw.m_Dictionary["ESP_REQ_PARM_COUNT"]=itoa(count, strVal, 10);

    while (--count>=0)
    {
        len=m_parameterList.GetText(count, strVal);
        if (len > 0 && strVal[len-1]=='>')
        {
            strVal[len-1]=0;
            char *pos=strchr(strVal, ' ');
            if (pos!=NULL)
            {
                *pos=0;
                pos+=2;
            }
        
            strKey.Format("ESP_REQ_PARM_%d", count);
            espaw.m_Dictionary[strKey]=strVal;
            strKey.Format("ESP_REQ_PARM_TYPE_%d", count);
            espaw.m_Dictionary[strKey]=param_type_map(pos);
        }
    }

    return TRUE;    // return FALSE if the dialog shouldn't be dismissed
}


BEGIN_MESSAGE_MAP(CEspRequestWizDlg, CAppWizStepDlg)
    //{{AFX_MSG_MAP(CEspRequestWizDlg)
    ON_BN_CLICKED(IDC_BTN_ADD_PARAMETER, OnBtnAddParameter)
    ON_BN_CLICKED(IDC_BTN_DELETE, OnBtnDelete)
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()


/////////////////////////////////////////////////////////////////////////////
// CEspRequestWizDlg message handlers



void CEspRequestWizDlg::OnBtnAddParameter() 
{
    UpdateData();
    
    CString addString;

    if (!m_parameterToAdd.IsEmpty())
    {
        addString.Format("%s <%s>", m_parameterToAdd, (m_typeToAdd<4) ? param_types[m_typeToAdd] : "string");

        if (m_parameterList.FindString(0, addString)==LB_ERR)
            m_parameterList.AddString(addString);
        m_parameterToAdd="";
    }

    UpdateData(FALSE);
}

void CEspRequestWizDlg::OnBtnDelete() 
{
    UpdateData();

    int item=m_parameterList.GetCurSel();
    if (item!=LB_ERR)
        m_parameterList.DeleteString(item);

    UpdateData(FALSE);
}
