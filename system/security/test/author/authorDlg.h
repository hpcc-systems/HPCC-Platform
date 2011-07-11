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
