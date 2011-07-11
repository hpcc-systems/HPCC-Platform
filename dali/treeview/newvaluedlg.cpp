/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
