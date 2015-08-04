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

// authorDlg.h : header file
//

#if !defined(AFX_AUTHORDLG_H__445B034B_5155_48AB_81BA_9F5E62B3D627__INCLUDED_)
#define AFX_AUTHORDLG_H__445B034B_5155_48AB_81BA_9F5E62B3D627__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include "seclib.hpp"

/////////////////////////////////////////////////////////////////////////////
// CAuthorDlg dialog

class CAuthorDlg : public CDialog
{
// Construction
public:
    CAuthorDlg(CWnd* pParent = NULL);   // standard constructor
    ~CAuthorDlg();

    void AddResPermissions(const char *name);
    void DisplayPermissions();

// Dialog Data
    //{{AFX_DATA(CAuthorDlg)
    enum { IDD = IDD_AUTHOR_DIALOG };
    BOOL    m_checkFeatureA;
    BOOL    m_checkFeatureB;
    BOOL    m_checkFeatureC;
    BOOL    m_checkFeatureD;
    BOOL    m_checkFeatureE;
    BOOL    m_checkFeatureF;
    BOOL    m_checkFeatureG;
    BOOL    m_checkFeatureH;
    BOOL    m_checkFeatureI;
    CString m_password;
    CString m_permissions;
    CString m_service;
    CString m_user;
    //}}AFX_DATA

    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CAuthorDlg)
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL

// Implementation
protected:
    HICON m_hIcon;

    // Generated message map functions
    //{{AFX_MSG(CAuthorDlg)
    virtual BOOL OnInitDialog();
    afx_msg void OnPaint();
    afx_msg HCURSOR OnQueryDragIcon();
    afx_msg void OnBtnAuthorize();
    afx_msg void OnBtnBuildFeatureSet();
    afx_msg void OnBtnCreateUser();
    afx_msg void OnBtnCreateSecMgr();
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()
private:
    Owned<ISecManager> m_sec_mgr;
    Owned<ISecResourceList> m_sec_resources;
    Owned<ISecUser> m_sec_user;


};

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_AUTHORDLG_H__445B034B_5155_48AB_81BA_9F5E62B3D627__INCLUDED_)
