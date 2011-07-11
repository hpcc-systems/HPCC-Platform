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

// treeviewDlg.h : header file
//

#if !defined(AFX_TREEVIEWDLG_H__57F384AE_7099_4A2B_A255_4C34441351B2__INCLUDED_)
#define AFX_TREEVIEWDLG_H__57F384AE_7099_4A2B_A255_4C34441351B2__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000


#include "jprop.hpp"
#include "afxtempl.h"
#include "inspectctrl.hpp"
#include "util.hpp"
#include "connect.hpp"

#define TREE_ENTRY_MAX_LABEL_SIZE 256


extern UINT WM_FINDREPLACE;


bool saveTree(LPCTSTR filename, IPropertyTree & pTree); // save to dat file
IPropertyTree * loadTree(LPCTSTR filename, bool xml = false);       // load from dat file


class CExpanderThread;

class CTreeviewDlg : public CDialog
{
protected:
    HACCEL hAccel;
    HICON m_hIcon;
    LPSTR cmdfname;
    // Generated message map functions
    //{{AFX_MSG(CTreeviewDlg)
    virtual BOOL OnInitDialog();
    afx_msg void OnSysCommand(UINT nID, LPARAM lParam);
    afx_msg void OnPaint();
    afx_msg HCURSOR OnQueryDragIcon();
    afx_msg void OnSize(UINT nType, int cx, int cy);
    afx_msg BOOL OnNextPrevField(UINT cmdID);
    afx_msg void OnClose();
    afx_msg void OnExit();
    afx_msg void OnMenuLoad();
    afx_msg void OnMenuSave();
    afx_msg void OnMenuExit();
    afx_msg void OnMenuExpandall();
    afx_msg void OnMenuContractall();
    afx_msg void OnMenuFind();
    afx_msg void OnMenuDelete();
    afx_msg void OnMenuDeleteConfirm();
    afx_msg void OnMenuAbout();
    afx_msg void OnMenuShowAttribs();
    afx_msg void OnMenuShowQualified();
    afx_msg LRESULT OnFindReplace(WPARAM wParam, LPARAM lParam);
    afx_msg void OnMenuConnectRemote();
    afx_msg void OnMenuReconnect();
    afx_msg void OnCommit();
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()

private:
    SocketEndpointArray connectedEpa;
    IConnection * connection;
    CFindReplaceDialog * findReplaceDialog;
    CString findStr;
    CToolTipCtrl * tooltipCtrl;
    long CloseRightOffset, CloseBottomOffset;
    long TreeRightMargin, TreeBottomMargin;
    CExpanderThread * expander;

    CPropertyInspector inspector;

    inline CWnd * GetDlgItemRect(int nId, RECT & rect);

    bool _loadTree(LPCSTR fname);
    bool _saveTree(LPCSTR fname);
    void setWindowTitle();
    void updateMenuState();
    void endExpand();   

public:
    CTreeviewDlg(LPCTSTR inFile, CWnd* pParent = NULL);
    ~CTreeviewDlg();

    //{{AFX_DATA(CTreeviewDlg)
    enum { IDD = IDD_TREEVIEW_DIALOG };
        // NOTE: the ClassWizard will add data members here
    //}}AFX_DATA

    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CTreeviewDlg)
    public:
    virtual BOOL PreTranslateMessage(MSG* pMsg);
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL

    void resetDialog();

friend class CExpanderThread;
};



/////////////////////////////////////////////////////////////////////////////
// CConnectDlg dialog

class CConnectDlg : public CDialog
{
// Construction
public:
    CConnectDlg(CWnd* pParent = NULL);   // standard constructor
    virtual ~CConnectDlg();

    int GetCovenSize();
    LPCSTR GetServerEndpoint(int idx);

// Dialog Data
    //{{AFX_DATA(CConnectDlg)
    enum { IDD = IDD_REMOTE_SERVER };
        // NOTE: the ClassWizard will add data members here
    //}}AFX_DATA


// Overrides
    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CConnectDlg)
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL

// Implementation
protected:

    // Generated message map functions
    //{{AFX_MSG(CConnectDlg)
    virtual void OnOK();
    virtual void OnCancel();
    virtual BOOL OnInitDialog();
    afx_msg void OnRemoveServerButton();
    afx_msg void OnAddServerButton();
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()
private:
    CArray<CString *, CString * &> ServerEndpoints;
    int CovenSize;

    int GetDlgItemInt(UINT DlgItem);
    void killEndpoints();
};




/////////////////////////////////////////////////////////////////////////////
// CEndpointDlg dialog

class CEndpointDlg : public CDialog
{
// Construction
public:
    LPCSTR GetEndpointStr();
    CEndpointDlg(CWnd* pParent = NULL);   // standard constructor

// Dialog Data
    //{{AFX_DATA(CEndpointDlg)
    enum { IDD = IDD_ENDPOINT_DIALOG };
        // NOTE: the ClassWizard will add data members here
    //}}AFX_DATA


// Overrides
    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CEndpointDlg)
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL

// Implementation
protected:

    // Generated message map functions
    //{{AFX_MSG(CEndpointDlg)
    virtual void OnOK();
    virtual void OnCancel();
    virtual BOOL OnInitDialog();
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()
private:
    CString Endpoint;
};
//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_TREEVIEWDLG_H__57F384AE_7099_4A2B_A255_4C34441351B2__INCLUDED_)
