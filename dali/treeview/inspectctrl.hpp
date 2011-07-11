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

#ifndef _inspectctrl_hpp
#define _inspectctrl_hpp


#define IDC_STATIC_CTRL         2000
#define IDC_EDIT_CTRL           2001
#define IDC_TREE_LIST_HEADER    2002
#define IDC_TREE_LIST_CTRL      2003


#include <afxtempl.h>
#include "jlib.hpp"
#include "jptree.hpp"

#include "connect.hpp"
#include "newvaluedlg.h" 

class CTreeListItem;
class CInspectorTreeCtrl;       // forward references

/////////////////////////////////////////////////////////////////////////////
// CEditEIP window

class CEditEIP : public CEdit
{
// Construction
public:
    CEditEIP();

// Attributes
public:

// Operations
public:

// Overrides
    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CEditEIP)
    public:
    //}}AFX_VIRTUAL

// Implementation
public:
    virtual ~CEditEIP();

    inline BOOL IsActive();
    BOOL Deactivate(BOOL save = TRUE);
    BOOL Activate(CTreeListItem * item, CRect & position);
    BOOL Resize(CRect & position);

    // Generated message map functions
protected:
    void SetFocusText(LPCSTR text);
    //{{AFX_MSG(CEditEIP)
    afx_msg int OnCreate(LPCREATESTRUCT lpCreateStruct);
    afx_msg void OnKeyUp(UINT nChar, UINT nRepCnt, UINT nFlags);
    //}}AFX_MSG

    DECLARE_MESSAGE_MAP()

private:
    CString ValuePreserve;
    CInspectorTreeCtrl * parent;
    CTreeListItem * tli;
};



/////////////////////////////////////////////////////////////////////////////

class CPropertyInspector;
class CFinderThread;

class CInspectorTreeCtrl : public CTreeCtrl
{
private:
    CMenu popupMenus;
    CFinderThread * finder;
    BOOL EIPActive;
    CTreeListItem * EIPItem;
    CEditEIP editCtrl;
    CToolTipCtrl * tooltipCtrl;
    CPen linePen;
    CBrush whitespaceHighlightBrush_Focused, whitespaceHighlightBrush_Unfocused;
    CBrush EIPBrush;

    void drawValues(CPaintDC & dc);
    HTREEITEM selectFromPoint(CPoint & point);
    void xpath(HTREEITEM item, CString & dest);
    void dynExpand(HTREEITEM in);
    bool matches(CTreeListItem * tli, LPCSTR findWhat, BOOL matchCase, BOOL wholeWord);
    HTREEITEM findNext(LPCSTR findWhat, BOOL matchCase, BOOL wholeWord, HTREEITEM from);
    void killDataItems(HTREEITEM in);
    CTreeListItem * GetTreeListItem(HTREEITEM in);
    bool GetNewItem(NewValue_t nvt, CString & name, CString & value, HTREEITEM & hParent);

protected:  
    // Generated message map functions
    //{{AFX_MSG(CInspectorTreeCtrl)
    afx_msg void OnPaint();
    afx_msg void OnSize(UINT type, int cx, int cy);
    afx_msg int OnCreate(LPCREATESTRUCT lpCreateStruct);
    afx_msg void OnLButtonDown(UINT nFlags, CPoint point);
    afx_msg void OnLButtonDblClk(UINT nFlags, CPoint point);
    afx_msg void OnKeyDown(UINT nChar, UINT nRepCnt, UINT nFlags);
    afx_msg void OnDestroy();
    afx_msg void OnLButtonUp(UINT flags, CPoint point);
    afx_msg void OnRButtonDown(UINT flags, CPoint point);
    afx_msg void OnRButtonUp(UINT flags, CPoint point);
    afx_msg void OnClose();
    afx_msg void OnVScroll(UINT nSBCode, UINT nPos, CScrollBar* pScrollBar);
    afx_msg BOOL OnMouseWheel(UINT nFlags, short zDelta, CPoint pt);
    afx_msg HBRUSH OnCtlColor(CDC* pCDC, CWnd* pWnd, UINT nCtlColor);
    afx_msg void OnItemExpanded(NMHDR* pNMHDR, LRESULT* pResult);
    afx_msg void OnGetDispInfo(NMHDR* pNMHDR, LRESULT* pResult);
    afx_msg void OnSetAsRoot();
    afx_msg void OnAddAttribute();
    afx_msg void OnAddProperty();
    afx_msg void OnDelete();
    afx_msg void OnHScroll(UINT nSBCode, UINT nPos, CScrollBar* pScrollBar);
    //}}AFX_MSG
    virtual LRESULT WindowProc(UINT Msg, WPARAM wParam, LPARAM lParam);
    void ActivateEIP(HTREEITEM i, CRect & position);
    void DeactivateEIP(BOOL save = TRUE);
    bool VScrollVisible();

    DECLARE_MESSAGE_MAP()

public:
    CInspectorTreeCtrl();
    ~CInspectorTreeCtrl();
    
    virtual BOOL DestroyWindow();

    int getColumnWidth(int idx);
    void setColumnWidth(int idx, int width);
    void columnSized();
    void NewTree(LPCSTR rootxpath = NULL);
    void DeleteCurrentItem(bool confirm=true);
    BOOL DeleteAllItems();
    DWORD getFullXPath(HTREEITEM item, CString& dest);
    void AddLevel(IPropertyTree & pTree, HTREEITEM hParent);

    void BeginFind();
    void NextFind(LPCSTR txt, BOOL MatchCase, BOOL MatchWholeWord);
    void EndFind();

friend class CPropertyInspector;
friend class CFinderThread;
};




class CPropertyInspector : public CWnd
{
private:
    CInspectorTreeCtrl inspectorCtrl;
    CHeaderCtrl headerCtrl;
    CStatic staticCtrl;

    static LONG FAR PASCAL wndProc(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam);

protected:
    virtual LRESULT WindowProc(UINT Msg, WPARAM wParam, LPARAM lParam);
    virtual BOOL OnNotify(WPARAM wParam, LPARAM lParam, LRESULT * result);

    afx_msg void OnSize(UINT type, int cx, int cy);

    DECLARE_MESSAGE_MAP()   

public:
    ~CPropertyInspector();

    static void registerClass();
    BOOL initialize();
    BOOL SubclassDlgItem(UINT id, CWnd * parent);
    void NewTree(IConnection * conn);
    void KillTree();
    UINT GetCount();
    CWnd * SetFocus();
    int getColumnWidth(int idx);
    void setColumnWidth(int colId, int width);
    void showAttribs(bool show = true);
    void showQualified(bool show = true);
    CInspectorTreeCtrl * getTree() { return &inspectorCtrl; }
    CHeaderCtrl * getHeader() { return &headerCtrl; }

    void BeginFind();
    void NextFind(LPCSTR txt, BOOL MatchCase, BOOL MatchWholeWord);
    void EndFind();
};

#endif


