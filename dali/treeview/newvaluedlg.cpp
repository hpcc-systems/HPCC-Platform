/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

// newvaluedlg.cpp : implementation file
//

#include "stdafx.h"
#include "treeview.h"
#include "newvaluedlg.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/////////////////////////////////////////////////////////////////////////////
// CNewValueDlg dialog


CNewValueDlg::CNewValueDlg(NewValue_t nvt, LPCSTR initName, LPCSTR initValue, CWnd* pParent) : CDialog(CNewValueDlg::IDD, pParent)
{
    //{{AFX_DATA_INIT(CNewValueDlg)
        // NOTE: the ClassWizard will add member initialization here
    //}}AFX_DATA_INIT
    NewValueType = nvt;
    Name = initName;
    Value = initValue;
}


void CNewValueDlg::DoDataExchange(CDataExchange* pDX)
{
    CDialog::DoDataExchange(pDX);
    //{{AFX_DATA_MAP(CNewValueDlg)
        // NOTE: the ClassWizard will add DDX and DDV calls here
    //}}AFX_DATA_MAP
}


BEGIN_MESSAGE_MAP(CNewValueDlg, CDialog)
    //{{AFX_MSG_MAP(CNewValueDlg)
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CNewValueDlg message handlers

BOOL CNewValueDlg::OnInitDialog() 
{
    CDialog::OnInitDialog();
    
    switch(NewValueType)
    {
    case NVT_property:  SetWindowText("New Property Details"); break;
    case NVT_attribute: SetWindowText("New Attribute Details"); break;
    }       

    CEdit * edt = static_cast <CEdit *> (GetDlgItem(IDC_NEWNAMEEDIT));  
    edt->SetWindowText(Name);
    edt = static_cast <CEdit *> (GetDlgItem(IDC_NEWVALUEEDIT));
    edt->SetWindowText(Value);

    return TRUE;  
}

LPCSTR CNewValueDlg::GetName()
{
    return Name;
}

LPCSTR CNewValueDlg::GetValue()
{
    return Value;
}

void CNewValueDlg::OnOK()
{
    CEdit * edt = static_cast <CEdit *> (GetDlgItem(IDC_NEWNAMEEDIT));  
    edt->GetWindowText(Name);
    edt = static_cast <CEdit *> (GetDlgItem(IDC_NEWVALUEEDIT));
    edt->GetWindowText(Value);

    if(Validate())
    {   
        CDialog::OnOK();
    }
}


bool CNewValueDlg::Validate()
{
    if(!Name.GetLength())
    {
        MessageBox(NewValueType == NVT_property ? "A property must have a name" : "An attribute must have a name", "Name Required", MB_OK);
        return false;
    }

    return true;
}
