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

// authorDlg.cpp : implementation file
//

#include "stdafx.h"
#include "author.h"
#include "authorDlg.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/////////////////////////////////////////////////////////////////////////////
// CAuthorDlg dialog

CAuthorDlg::CAuthorDlg(CWnd* pParent /*=NULL*/)
    : CDialog(CAuthorDlg::IDD, pParent)
{
    //{{AFX_DATA_INIT(CAuthorDlg)
    m_checkFeatureA = FALSE;
    m_checkFeatureB = FALSE;
    m_checkFeatureC = FALSE;
    m_checkFeatureD = FALSE;
    m_checkFeatureE = FALSE;
    m_checkFeatureF = FALSE;
    m_checkFeatureG = FALSE;
    m_checkFeatureH = FALSE;
    m_checkFeatureI = FALSE;
    m_password = _T("");
    m_permissions = _T("");
    m_service = _T("");
    m_user = _T("");
    //}}AFX_DATA_INIT
    m_hIcon = AfxGetApp()->LoadIcon(IDR_MAINFRAME);
    
    
    m_service = AfxGetApp()->GetProfileString("defaults", "service_name", "MyService");
    m_user = AfxGetApp()->GetProfileString("defaults", "user_name", "jsmith");
    m_password = AfxGetApp()->GetProfileString("defaults", "password", "password");
}

CAuthorDlg::~CAuthorDlg()
{
    AfxGetApp()->WriteProfileString("defaults", "service_name", m_service);
    AfxGetApp()->WriteProfileString("defaults", "user_name", m_user);
    AfxGetApp()->WriteProfileString("defaults", "password", m_password);
}

void CAuthorDlg::DoDataExchange(CDataExchange* pDX)
{
    CDialog::DoDataExchange(pDX);
    //{{AFX_DATA_MAP(CAuthorDlg)
    DDX_Check(pDX, IDC_FEATURE_A, m_checkFeatureA);
    DDX_Check(pDX, IDC_FEATURE_B, m_checkFeatureB);
    DDX_Check(pDX, IDC_FEATURE_C, m_checkFeatureC);
    DDX_Check(pDX, IDC_FEATURE_D, m_checkFeatureD);
    DDX_Check(pDX, IDC_FEATURE_E, m_checkFeatureE);
    DDX_Check(pDX, IDC_FEATURE_F, m_checkFeatureF);
    DDX_Check(pDX, IDC_FEATURE_G, m_checkFeatureG);
    DDX_Check(pDX, IDC_FEATURE_H, m_checkFeatureH);
    DDX_Check(pDX, IDC_FEATURE_I, m_checkFeatureI);
    DDX_Text(pDX, IDC_PASSWORD, m_password);
    DDX_Text(pDX, IDC_PERMISSIONS, m_permissions);
    DDX_Text(pDX, IDC_SERVICE_NAME, m_service);
    DDX_Text(pDX, IDC_USER, m_user);
    //}}AFX_DATA_MAP
}

BEGIN_MESSAGE_MAP(CAuthorDlg, CDialog)
    //{{AFX_MSG_MAP(CAuthorDlg)
    ON_WM_PAINT()
    ON_WM_QUERYDRAGICON()
    ON_BN_CLICKED(IDC_BTN_AUTHORIZE, OnBtnAuthorize)
    ON_BN_CLICKED(IDC_BTN_BUILD_FEATURE_SET, OnBtnBuildFeatureSet)
    ON_BN_CLICKED(IDC_BTN_CREATE_USER, OnBtnCreateUser)
    ON_BN_CLICKED(IDC_BTN_CREATE_SEC_MGR, OnBtnCreateSecMgr)
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CAuthorDlg message handlers

BOOL CAuthorDlg::OnInitDialog()
{
    CDialog::OnInitDialog();

    SetIcon(m_hIcon, TRUE);         // Set big icon
    SetIcon(m_hIcon, FALSE);        // Set small icon
    
    return TRUE;  // return TRUE  unless you set the focus to a control
}


void CAuthorDlg::OnPaint() 
{
    if (IsIconic())
    {
        CPaintDC dc(this); // device context for painting

        SendMessage(WM_ICONERASEBKGND, (WPARAM) dc.GetSafeHdc(), 0);

        CRect rect;
        GetClientRect(&rect);
        
        int x = (rect.Width() - GetSystemMetrics(SM_CXICON) + 1) / 2;
        int y = (rect.Height() - GetSystemMetrics(SM_CYICON) + 1) / 2;

        // Draw the icon
        dc.DrawIcon(x, y, m_hIcon);
    }
    else
    {
        CDialog::OnPaint();
    }
}

HCURSOR CAuthorDlg::OnQueryDragIcon()
{
    return (HCURSOR) m_hIcon;
}

void CAuthorDlg::OnBtnBuildFeatureSet() 
{
    UpdateData();
    
    m_sec_resources.set(m_sec_mgr->createResourceList(NULL));

    if (m_sec_resources)
    {
        if (m_checkFeatureA)
            m_sec_resources->addResource("FeatureA");
        if (m_checkFeatureB)
            m_sec_resources->addResource("FeatureB");
        if (m_checkFeatureC)
            m_sec_resources->addResource("FeatureC");
        if (m_checkFeatureD)
            m_sec_resources->addResource("FeatureD");
        if (m_checkFeatureE)
            m_sec_resources->addResource("FeatureE");
        if (m_checkFeatureF)
            m_sec_resources->addResource("FeatureF");
        if (m_checkFeatureG)
            m_sec_resources->addResource("FeatureG");
        if (m_checkFeatureH)
            m_sec_resources->addResource("FeatureH");
        if (m_checkFeatureI)
            m_sec_resources->addResource("FeatureI");
    }

}

void CAuthorDlg::OnBtnCreateUser() 
{
    UpdateData();

    m_sec_user.set(m_sec_mgr->createUser(m_user));

    if (m_sec_user)
    {
        m_sec_user->credentials().setPassword(m_password);
    }
}


void CAuthorDlg::OnBtnCreateSecMgr() 
{
    UpdateData();
    
    m_sec_mgr.set(createSecManager("secdemo", m_service, NULL));
}


void CAuthorDlg::OnBtnAuthorize() 
{
    UpdateData();


    if (m_sec_mgr && m_sec_user && m_sec_resources)
    {
        if (m_sec_mgr->authorize(*m_sec_user.get(), m_sec_resources.get()))
        {
            DisplayPermissions();
        }
    }
}

void CAuthorDlg::DisplayPermissions()
{
    m_permissions = "";

    AddResPermissions("FeatureA");
    AddResPermissions("FeatureB");
    AddResPermissions("FeatureC");
    AddResPermissions("FeatureD");
    AddResPermissions("FeatureE");
    AddResPermissions("FeatureF");
    AddResPermissions("FeatureG");
    AddResPermissions("FeatureH");
    AddResPermissions("FeatureI");

    UpdateData(FALSE);
}



char *perm_descr[]=
{  
    " Owner "
    " Read ",
    " Write ",
    " Execute ",
    NULL
};

unsigned allperms = SecAccess_Full;

void CAuthorDlg::AddResPermissions(const char *name)
{
    ISecResource *res = m_sec_resources->getResource(name);
    
    if (res)
    {
        unsigned flags = res->getAccessFlags();

        if ((flags & allperms))
        {
            m_permissions += res->getName();
            m_permissions += " = ";

            unsigned attr = 1;

            int index = 0;
            while (perm_descr[index] != NULL)
            {
                if (flags & (1<<index))
                {
                    m_permissions += perm_descr[index];
                }

                index++;
            }
    
            m_permissions += "\r\n";
        }
    }
}
