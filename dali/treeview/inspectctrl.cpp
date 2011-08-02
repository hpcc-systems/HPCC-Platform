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



#include "stdafx.h"
#include "inspectctrl.hpp"
#include "resource.h"
#include "newvaluedlg.h"

#include "jthread.hpp"

#define MAX_PROPERTY_VALUE_SIZE     256
#define TREE_TOOLTIP_ID             100
#define IDC_EIP_CTRL                1099


enum TreeList_t { TLT_property, TLT_attribute, TLT_root };


const char binaryValue[]    = "[BINARY]";

const UINT MSG_COLUMN_SIZED = 7000;
const UINT MSG_EIP_RESIZE   = 7001;


bool ShowAttrsOnProps = true;   
bool ShowQualifiedNames = false;

IConnection * connection = NULL;        // the current connection

const COLORREF color_highlight      = GetSysColor(COLOR_HIGHLIGHT);
const COLORREF color_window         = GetSysColor(COLOR_WINDOW);
const COLORREF color_windowtext     = GetSysColor(COLOR_WINDOWTEXT);
const COLORREF color_inactiveborder = GetSysColor(COLOR_INACTIVEBORDER);
const COLORREF color_highlighttext  = GetSysColor(COLOR_HIGHLIGHTTEXT);
const COLORREF color_eip_back       = RGB(210, 210, 240);





#define STRCAT(dest, destSz, src) {size32_t l = strlen(src); if(destSz > l) { strcat(dest, src); destSz -= l; } }

enum CTState { CTS_None, CTS_Visible = 0x01, CTS_Expanded = 0x02 };
class CTreeListItem 
{
private:
    TreeList_t type;
    char * name;
    IPropertyTree * pTree;      
    byte state;

public: 
    CTreeListItem(LPCSTR _name, IPropertyTree * _pTree, TreeList_t _type)
    {
        state = CTS_None;
        type = _type;
        pTree =_pTree;
        name = type == TLT_attribute ? strdup(_name) : NULL;
    }

    ~CTreeListItem() { if(name) free(name); }

    inline int getDisplayName(IPropertyTree * parent, char * buffer, size32_t buffsz) const
    {
        *buffer = 0;
        size32_t insz = buffsz; 
        switch(type)
        {
        case TLT_root:
        case TLT_property:
            STRCAT(buffer, buffsz, "  ");
            STRCAT(buffer, buffsz, getName(parent));
            if(ShowAttrsOnProps)
            {
                bool first = true;
                IAttributeIterator * attrIterator = pTree->getAttributes();
                attrIterator->first();
                while(attrIterator->isValid())
                {
                    STRCAT(buffer, buffsz, "  ");                   
                    if(first)
                    {
                        STRCAT(buffer, buffsz, "<");
                        first = false;
                    }
                    STRCAT(buffer, buffsz, attrIterator->queryName() + 1);
                    STRCAT(buffer, buffsz, "=");                    
                    STRCAT(buffer, buffsz, attrIterator->queryValue());
                    attrIterator->next();
                }
                attrIterator->Release();
                if(!first) STRCAT(buffer, buffsz, ">");
            }
            break;
        case TLT_attribute:
            STRCAT(buffer, buffsz, "= ");
            STRCAT(buffer, buffsz, getName(NULL));
            break;
        }
        return insz - buffsz;
    }

    bool setValue(LPCSTR newValue)          // returns true if value actually updated
    {
        ASSERT(!isBinary());
        ASSERT(connection != NULL);
        if(connection->lockWrite())
        {
            pTree->setProp(name, newValue);
            connection->unlockWrite();
            return true;
        }
        return false;       
    }

    inline bool isBinary() const { return pTree->isBinary(name); }  
    inline bool isExpanded() const { return 0 != (state & CTS_Expanded); }
    inline bool isVisible() const { return 0 != (state & CTS_Visible); }
    inline void setExpanded() { state += CTS_Expanded; }
    inline void setVisible() { state += CTS_Visible; }
    inline IPropertyTree *queryPropertyTree() const { return pTree; } 

    inline LPCSTR getName(IPropertyTree * parent, bool forceQualified = false) const 
    { 
        if(type != TLT_attribute && parent && (ShowQualifiedNames || forceQualified))
        {
            static StringBuffer buf;
            buf.clear().append(type == TLT_attribute ? name + 1 : pTree->queryName());
            buf.append("[");
            buf.append(parent->queryChildIndex(pTree) + 1);
            buf.append("]");
            return buf.toCharArray();
        }
        return type == TLT_attribute ? name + 1 : pTree->queryName(); 
    }

    inline LPCSTR getValue() const
    {
        static StringBuffer buf;
        if(isBinary())      
            buf.clear().append(binaryValue);        
        else        
            pTree->getProp(name, buf.clear());      

        return buf.toCharArray();
    }

    inline TreeList_t getType() const { return type; }
};


CTreeListItem * createTreeListProperty(LPCSTR PropName, IPropertyTree & pTree)
{ 
    return new CTreeListItem(PropName, &pTree, TLT_property); 
}

CTreeListItem * createTreeListAttribute(LPCSTR AttrName, IPropertyTree & pTree)
{
    return new CTreeListItem(AttrName, &pTree, TLT_attribute); 
}

CTreeListItem * createTreeListRoot(LPCSTR RootName, IPropertyTree & pTree)
{
    return new CTreeListItem(RootName, &pTree, TLT_root);
}









class CFinderThread : public Thread
{
private:
    CInspectorTreeCtrl & tree;
    LPSTR findWhat;
    BOOL matchCase;
    BOOL wholeWord;
    Semaphore hold;
    bool terminate;
    HTREEITEM matchedItem;

    bool matches(CTreeListItem * tli)
    {
        LPCSTR name = tli->getName(NULL), value = tli->getValue();
        if(matchCase)
        {
            if(strstr(name, findWhat)) return true;
            if(value && !tli->isBinary() && strstr(value, findWhat)) return true;           
        }
        else
        {
            static CString what, nbuf;
            what = findWhat;
            nbuf = name;
            what.MakeUpper();
            nbuf.MakeUpper();
            if(strstr(nbuf, what)) return true;     
            if(value && !tli->isBinary())
            {
                static CString vbuf;
                vbuf= value;
                vbuf.MakeUpper();
                if(strstr(vbuf, what)) return true;
            }
        }
        return false;
    }

public:
    CFinderThread(CInspectorTreeCtrl &_tree, LPCSTR _findWhat, BOOL _matchCase, BOOL _wholeWord) : tree(_tree) 
    {
        findWhat = strdup(_findWhat);
        matchCase = _matchCase;
        wholeWord = _wholeWord;
        terminate = false;
        matchedItem = 0;
    }

    ~CFinderThread()
    {
        free(findWhat);
    }

    void process(HTREEITEM in)
    {       
        if(!terminate)
        {
            CTreeListItem * tli = tree.GetTreeListItem(in);
            if(tli->getType() == TLT_property && !tli->isExpanded()) tree.AddLevel(*tli->queryPropertyTree(), in);                      
            if(matches(tli))
            {
                tree.EnsureVisible(in);
                tree.SelectItem(in);
                hold.wait();
                if(terminate) return;
            }   
            HTREEITEM i = tree.GetChildItem(in);        
            while(i)
            {
                process(i);
                if(terminate) break;
                i = tree.GetNextItem(i, TVGN_NEXT);             
            }           
        }
    }

    void kill()         // called on message thread
    {
        terminate = true;
        hold.signal();
    }

    void next()
    {
        hold.signal();
    }

    virtual int run()
    {
        process(tree.GetRootItem());
        if(!terminate) MessageBox(NULL, "Search finished, no more matches", "Search Complete", MB_OK | MB_ICONHAND);
        return 0;
    }
};





inline void getTypeText(TreeList_t type, CString & dest, bool forToolTip)
{
    switch(type)
    {
    case TLT_property:  dest = "Property"; break;
    case TLT_attribute: dest = "Attribute"; break;
    case TLT_root:      dest = "Root"; break;
    }
    if(forToolTip) dest += ": ";
}



// ----- Inspector tree control --------------------------------------------------


BEGIN_MESSAGE_MAP(CInspectorTreeCtrl, CTreeCtrl)
    //{{AFX_MSG_MAP(CInspectorTreeCtrl)
    ON_WM_PAINT()
    ON_WM_SIZE()
    ON_WM_CREATE()
    ON_WM_LBUTTONDOWN()
    ON_WM_LBUTTONDBLCLK()
    ON_WM_KEYDOWN()
    ON_WM_DESTROY()
    ON_WM_LBUTTONUP()
    ON_WM_RBUTTONDOWN()
    ON_WM_RBUTTONUP()
    ON_WM_CLOSE()
    ON_WM_VSCROLL()
    ON_WM_MOUSEWHEEL()
    ON_WM_CTLCOLOR()
    ON_NOTIFY_REFLECT(TVN_ITEMEXPANDED, OnItemExpanded)
    ON_NOTIFY_REFLECT(TVN_GETDISPINFO, OnGetDispInfo)
    ON_COMMAND(IDMP_SETASROOT, OnSetAsRoot)
    ON_COMMAND(IDMP_ADDATTRIBUTE, OnAddAttribute)
    ON_COMMAND(IDMP_ADDPROPERTY, OnAddProperty)
    ON_COMMAND(IDMP_DELETE, OnDelete)
    ON_WM_HSCROLL()
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()


CInspectorTreeCtrl::CInspectorTreeCtrl()
{
    linePen.CreatePen(PS_SOLID, 0, color_inactiveborder);
    whitespaceHighlightBrush_Focused.CreateSolidBrush(color_highlight); 
    whitespaceHighlightBrush_Unfocused.CreateSolidBrush(color_inactiveborder);
    EIPBrush.CreateSolidBrush(color_eip_back);

    popupMenus.LoadMenu(IDR_POPUPMENUS);

    EIPActive = false;
    connection = NULL;
    finder = NULL;
}


CInspectorTreeCtrl::~CInspectorTreeCtrl()
{
    if(finder) delete finder;
}


void CInspectorTreeCtrl::killDataItems(HTREEITEM in)
{
    if(in)
    {
        HTREEITEM i = GetChildItem(in);     
        while(i)
        {
            killDataItems(i);
            i = GetNextItem(i, TVGN_NEXT);
        }           
        CTreeListItem * tli = GetTreeListItem(in);
        SetItemData(in, 0);
        delete tli;
    }
}


LRESULT CInspectorTreeCtrl::WindowProc(UINT Msg, WPARAM wParam, LPARAM lParam)
{
    switch(Msg)
    {
    case MSG_EIP_RESIZE:
        HTREEITEM hitItem = GetSelectedItem();
        CRect rect;
        GetItemRect(hitItem, &rect, FALSE);
        rect.left = getColumnWidth(0);
        rect.right = rect.left + getColumnWidth(1);
        if(rect.Width() > 0)
            editCtrl.Resize(rect);
        else
            DeactivateEIP();
        return 0;
    }
    return CTreeCtrl::WindowProc(Msg, wParam, lParam);
}


void CInspectorTreeCtrl::DeleteCurrentItem(bool confirm) 
{
    HTREEITEM hItem = GetSelectedItem();
    if (hItem)
    {
        if(connection->lockWrite())
        {           
            IPropertyTree * pTree = NULL;
            CString name;

            CTreeListItem * tli = GetTreeListItem(hItem);
            if(tli->getType() == TLT_property)
            {
                if (!confirm || MessageBox("Delete property, are you sure?", "Delete Confirmation", MB_ICONQUESTION | MB_YESNO) == IDYES)
                {                       
                    CTreeListItem * parentTli = GetTreeListItem(GetParentItem(hItem));                  
                    pTree = parentTli->queryPropertyTree();
                    name = tli->getName(pTree, true);
                }
            }
            else
            {
                if (!confirm || MessageBox("Delete attribute, are you sure?", "Delete Confirmation", MB_ICONQUESTION | MB_YESNO) == IDYES)
                {
                    pTree = tli->queryPropertyTree();
                    name = "@";
                    name += tli->getName(NULL);
                }
            }       
            if(pTree)
            {
                if(pTree->removeProp(name))
                {
                    killDataItems(hItem);
                    DeleteItem(hItem);
                }
                else
                {
                    MessageBox("Failed to remove property or attribute, removeProp()\nfailed", "Failed to Remove", MB_OK | MB_ICONEXCLAMATION);
                }
            
            }   
            connection->unlockWrite();
        }
        else
            MessageBox("Unable to lock connection for write", "Cannot Obtain Lock", MB_OK);         
    }
}

BOOL CInspectorTreeCtrl::DeleteAllItems()
{
    DeactivateEIP();
    killDataItems(GetRootItem());
    CTreeCtrl::DeleteAllItems();
    return TRUE;
}


CTreeListItem * CInspectorTreeCtrl::GetTreeListItem(HTREEITEM in)
{
    return in ? reinterpret_cast <CTreeListItem *> (GetItemData(in)) : NULL;
}


void CInspectorTreeCtrl::xpath(HTREEITEM item, CString & dest)
{
    HTREEITEM parent = GetParentItem(item);
    if(parent) xpath(parent, dest);             // recursion
        
    CTreeListItem * i = GetTreeListItem(item);
    if(i->getType() != TLT_root)
    {
        if(parent && GetTreeListItem(parent)->getType() != TLT_root) dest += "/";   

        IPropertyTree * ppTree = GetTreeListItem(parent)->queryPropertyTree();
        if(i->getType() == TLT_attribute) dest += "@";
        dest += i->getName(ppTree, true);
    }
}


DWORD CInspectorTreeCtrl::getFullXPath(HTREEITEM item, CString & dest)
{
    xpath(item, dest);
    return dest.GetLength();
}


BOOL CInspectorTreeCtrl::DestroyWindow()
{   
    DeactivateEIP();
    DeleteAllItems();
    return CWnd::DestroyWindow();
}


bool CInspectorTreeCtrl::VScrollVisible()
{
    int sMin, sMax;                             // crafty eh?       
    GetScrollRange(SB_VERT, &sMin, &sMax);
    return sMax != 0;
}

int CInspectorTreeCtrl::getColumnWidth(int idx)
{
    int r = (reinterpret_cast <CPropertyInspector *> (GetParent()))->getColumnWidth(idx);
    if(idx == 1 && VScrollVisible()) r -= GetSystemMetrics(SM_CXVSCROLL);
    return r > 0 ? r : 0;
}

void CInspectorTreeCtrl::setColumnWidth(int idx, int wid)
{
    (reinterpret_cast <CPropertyInspector *> (GetParent()))->setColumnWidth(idx, wid);
}


#define DRAWLINE(x1, y1, x2, y2) dc.MoveTo(x1, y1); dc.LineTo(x2, y2) 

void CInspectorTreeCtrl::drawValues(CPaintDC & dc)
{
    CRect rect;
    GetWindowRect(rect);

    dc.SetViewportOrg(0, 0);
    dc.SelectObject(linePen);
    dc.SetBkMode(TRANSPARENT);
    dc.SelectObject(GetFont());
    dc.SetTextColor(color_windowtext);  

    DRAWLINE(0, 0, rect.right, 0);

    int cWid0 = getColumnWidth(0);
    int cWid1 = getColumnWidth(1);

    int height = 0;
    HTREEITEM hItemFocus = GetSelectedItem();
    HTREEITEM hItem = GetFirstVisibleItem();
    while(hItem && height < rect.Height())
    {
        CRect iRect;
        GetItemRect(hItem, &iRect, FALSE);

        DRAWLINE(0, iRect.bottom, rect.right, iRect.bottom);
        height += iRect.Height();

        CTreeListItem * itemData = GetTreeListItem(hItem);
        if(itemData)
        {
            iRect.left = cWid0 + 6;
            iRect.right = cWid0 + cWid1;
            
            if(hItem == hItemFocus)
            {
                CRect whitespaceRect;
                GetItemRect(hItem, &whitespaceRect, TRUE);

                if(whitespaceRect.right < cWid0)
                {
                    whitespaceRect.left = whitespaceRect.right;
                    whitespaceRect.right = cWid0;
                
                    CWnd * focusWnd = GetFocus();
                    if(focusWnd && (focusWnd->m_hWnd == m_hWnd))        // I have focus             
                        dc.FillRect(whitespaceRect, &whitespaceHighlightBrush_Focused);                 
                    else                
                        dc.FillRect(whitespaceRect, &whitespaceHighlightBrush_Unfocused);                                   
                }               
                CString xpath;
                getTypeText(itemData->getType(), xpath, true);
                if(getFullXPath(hItem, xpath)) 
                {
                    CRect itemRect, r;
                    GetItemRect(hItem, &itemRect, FALSE);
                    r.UnionRect(&itemRect, &whitespaceRect);
                    tooltipCtrl->DelTool(this, TREE_TOOLTIP_ID);
                    tooltipCtrl->AddTool(this, xpath, r, TREE_TOOLTIP_ID);
                }
            }
            dc.DrawText(itemData->getValue(), &iRect, DT_SINGLELINE | DT_LEFT);
        }
        hItem = GetNextVisibleItem(hItem);      
    }
    DRAWLINE(cWid0, 0, cWid0, height);
}


void CInspectorTreeCtrl::OnSize(UINT type, int cx, int cy)
{
    CWnd::OnSize(type, cx, cy); 
    if(EIPActive) PostMessage(MSG_EIP_RESIZE);
}


void CInspectorTreeCtrl::OnPaint()
{   
    CPaintDC dc(this); 

    CPropertyInspector * parent = static_cast <CPropertyInspector *> (GetParent());

    CRect rcClip;
    dc.GetClipBox( &rcClip );       
    rcClip.right = getColumnWidth(0);

    CRgn rgn;                   
    rgn.CreateRectRgnIndirect( &rcClip );
    dc.SelectClipRgn(&rgn);

    CWnd::DefWindowProc(WM_PAINT, reinterpret_cast <WPARAM> (dc.m_hDC), 0);     // let CTreeCtrl paint as normal
    rgn.DeleteObject();

    rcClip.right += parent->getColumnWidth(1);
    rgn.CreateRectRgnIndirect( &rcClip );
    dc.SelectClipRgn(&rgn);

    drawValues(dc);

    rgn.DeleteObject();
}


int CInspectorTreeCtrl::OnCreate(LPCREATESTRUCT createStruct)
{
    if(CTreeCtrl::OnCreate(createStruct) == -1) return -1;

    tooltipCtrl = GetToolTips();
    ASSERT(tooltipCtrl != NULL);
    if(!editCtrl.Create(WS_CHILD | WS_BORDER | ES_AUTOHSCROLL, CRect(0, 0, 0, 0), this, IDC_EIP_CTRL)) return -1;

    return 0;
}


HTREEITEM CInspectorTreeCtrl::selectFromPoint(CPoint & point)
{
    HTREEITEM hitItem = HitTest(point);
    if(hitItem && hitItem != GetSelectedItem()) SelectItem(hitItem);    
    return hitItem;
}


void CInspectorTreeCtrl::OnLButtonDown(UINT flags, CPoint point)
{
    DeactivateEIP();
    HTREEITEM hitItem = HitTest(point);
    if(hitItem && hitItem != GetSelectedItem()) selectFromPoint(point); 
    CTreeCtrl::OnLButtonDown(flags, point);
}


void CInspectorTreeCtrl::DeactivateEIP(BOOL save)
{   
    if(editCtrl.Deactivate(save)) Invalidate(); 
}


void CInspectorTreeCtrl::ActivateEIP(HTREEITEM i, CRect & rect)
{
    ASSERT(connection != NULL);
    DeactivateEIP();
    EIPActive = editCtrl.Activate(GetTreeListItem(i), rect);
}


void CInspectorTreeCtrl::OnLButtonUp(UINT flags, CPoint point)
{
    DeactivateEIP();
    HTREEITEM hitItem = HitTest(point);
    if(hitItem && hitItem == GetSelectedItem())
    {
        CRect rect;
        GetItemRect(hitItem, &rect, FALSE);
        rect.left = getColumnWidth(0);
        rect.right = rect.left + getColumnWidth(1);
        if(rect.PtInRect(point)) ActivateEIP(hitItem, rect);        
    }   
    CTreeCtrl::OnLButtonUp(flags, point);
}


void CInspectorTreeCtrl::OnRButtonDown(UINT flags, CPoint point)
{
    /* NULL */
}


void CInspectorTreeCtrl::OnRButtonUp(UINT flags, CPoint point)
{
    HTREEITEM sel = selectFromPoint(point);
    if(sel)
    {
        ClientToScreen(&point);
        CMenu * sm = popupMenus.GetSubMenu(0);

        switch(GetTreeListItem(sel)->getType())
        {
        case TLT_root:
            sm->EnableMenuItem(IDMP_ADDPROPERTY, MF_ENABLED);
            sm->EnableMenuItem(IDMP_ADDATTRIBUTE, MF_ENABLED);
            sm->EnableMenuItem(IDMP_DELETE, MF_GRAYED);
            sm->EnableMenuItem(IDMP_SETASROOT, MF_GRAYED);
            break;
        case TLT_property:
            sm->EnableMenuItem(IDMP_ADDPROPERTY, MF_ENABLED);
            sm->EnableMenuItem(IDMP_ADDATTRIBUTE, MF_ENABLED);
            sm->EnableMenuItem(IDMP_DELETE, MF_ENABLED);
            sm->EnableMenuItem(IDMP_SETASROOT, MF_ENABLED);
            break;
        case TLT_attribute:
            sm->EnableMenuItem(IDMP_ADDPROPERTY, MF_GRAYED);
            sm->EnableMenuItem(IDMP_ADDATTRIBUTE, MF_GRAYED);
            sm->EnableMenuItem(IDMP_DELETE, MF_ENABLED);
            sm->EnableMenuItem(IDMP_SETASROOT, MF_GRAYED);
            break;
        default:
            ASSERT(FALSE);
        }
        sm->TrackPopupMenu(TPM_LEFTALIGN | TPM_RIGHTBUTTON, point.x, point.y, this);
    }
}


void CInspectorTreeCtrl::OnLButtonDblClk(UINT flags, CPoint point)
{
    CTreeCtrl::OnLButtonDblClk(flags, point);
}


void CInspectorTreeCtrl::OnKeyDown(UINT chr, UINT repCnt, UINT flags)
{
    CTreeCtrl::OnKeyDown(chr, repCnt, flags);
}


void CInspectorTreeCtrl::OnDestroy()
{
    DeactivateEIP(FALSE);
    DeleteAllItems();
    CTreeCtrl::OnDestroy();
}


void CInspectorTreeCtrl::OnClose()
{
    DeactivateEIP();
    CTreeCtrl::OnClose();
}


void CInspectorTreeCtrl::OnVScroll(UINT nSBCode, UINT nPos, CScrollBar* pScrollBar) 
{
    DeactivateEIP();    
    CTreeCtrl::OnVScroll(nSBCode, nPos, pScrollBar);
}


BOOL CInspectorTreeCtrl::OnMouseWheel(UINT nFlags, short zDelta, CPoint pt) 
{
    DeactivateEIP();    
    return CTreeCtrl::OnMouseWheel(nFlags, zDelta, pt);
}

void CInspectorTreeCtrl::OnGetDispInfo(NMHDR* pNMHDR, LRESULT* pResult) 
{
    TV_DISPINFO * pTVDispInfo = reinterpret_cast <TV_DISPINFO *> (pNMHDR);

    CTreeListItem * tli = reinterpret_cast <CTreeListItem *> (pTVDispInfo->item.lParam);
    if(tli)
    {
        static char nameBuffer[256];

        IPropertyTree * ppTree = tli->getType() == TLT_root ? NULL : GetTreeListItem(GetParentItem(pTVDispInfo->item.hItem))->queryPropertyTree();
        int origlen = tli->getDisplayName(ppTree, nameBuffer , sizeof(nameBuffer));
        
        HTREEITEM parent = NULL;
        CDC * dc = GetDC();
        dc->SetViewportOrg(0, 0);
        dc->SelectObject(GetFont());
        int fit;
        CSize sz;
        CRect rect;
        if(GetItemRect(pTVDispInfo->item.hItem, &rect, TRUE))
        {
            rect.right = getColumnWidth(0);
            GetTextExtentExPoint(dc->m_hDC, nameBuffer, origlen, rect.Width() - 2, &fit, NULL, &sz);

            if(fit < origlen)
            {
                if(fit > 3)
                {
                    strcpy(&nameBuffer[fit - 3], "...");
                    pTVDispInfo->item.pszText = nameBuffer;
                }
                else
                {
                    pTVDispInfo->item.pszText = NULL;
                }
            }
            else
            {
                pTVDispInfo->item.pszText = nameBuffer; 
            }
        }
        else
        {
            pTVDispInfo->item.pszText = NULL;   
        }
        ReleaseDC(dc);
    }
    *pResult = 0;
}

HBRUSH CInspectorTreeCtrl::OnCtlColor(CDC* pDC, CWnd* pWnd, UINT nCtlColor)
{
    HBRUSH hbr = CTreeCtrl::OnCtlColor(pDC, pWnd, nCtlColor);  

    if(pWnd->GetDlgCtrlID() == IDC_EIP_CTRL)
    {
        if(nCtlColor == CTLCOLOR_EDIT || nCtlColor == CTLCOLOR_MSGBOX)
        {
            pDC->SetBkColor(color_eip_back);
            return EIPBrush;                
        } 
    }    
    return hbr;
}

void CInspectorTreeCtrl::NewTree(LPCSTR rootxpath)
{
    ASSERT(connection != NULL);
    DeleteAllItems();
    IPropertyTree & pTree = *connection->queryRoot(rootxpath);
    if(connection->getType() != CT_none)
    {
        TV_INSERTSTRUCT is;
        is.hInsertAfter = TVI_LAST;
        is.item.mask = TVIF_TEXT | TVIF_PARAM;
        is.item.pszText = LPSTR_TEXTCALLBACK;           
        is.hParent = TVI_ROOT;
        is.item.lParam = reinterpret_cast <DWORD> (createTreeListRoot(pTree.queryName(), pTree));
        HTREEITEM r = InsertItem(&is);
        AddLevel(pTree, r);
        Expand(r, TVE_EXPAND);
    }   
}


void CInspectorTreeCtrl::dynExpand(HTREEITEM in)
{
    CTreeListItem *parent = GetTreeListItem(in);
    assertex(parent);
    IPropertyTree &pTree = *parent->queryPropertyTree();
    if (!parent->isExpanded())
    {
        HTREEITEM i = GetChildItem(in);
        while(i)
        {
            DeleteItem(i);
            i = GetNextItem(i, TVGN_NEXT);              
        }
        CString txt;
        TV_INSERTSTRUCT is;
        is.hInsertAfter = TVI_LAST;
        is.item.mask = TVIF_TEXT | TVIF_PARAM;
        is.item.pszText = LPSTR_TEXTCALLBACK;   
        
        Owned<IAttributeIterator> attrIterator = pTree.getAttributes();
        ForEach(*attrIterator)
        {
            is.hParent = in;
            is.item.lParam = reinterpret_cast <DWORD> (createTreeListAttribute(attrIterator->queryName(), pTree));
            HTREEITEM r = InsertItem(&is);
            ASSERT(r != NULL);
        }

        Owned<IPropertyTreeIterator> iterator = pTree.getElements("*", iptiter_sort);
        ForEach(*iterator)
        {
            IPropertyTree & thisTree = iterator->query();

            is.hParent = in;
            is.item.lParam = reinterpret_cast <DWORD> (createTreeListProperty(thisTree.queryName(), thisTree));
            HTREEITEM thisTreeItem = InsertItem(&is);
            ASSERT(thisTreeItem != NULL);
        }
        parent->setExpanded();
    }

    HTREEITEM i = GetChildItem(in);     
    while(i)
    {
        CTreeListItem * ctli = GetTreeListItem(i);

        if(ctli->getType() == TLT_property) AddLevel(*ctli->queryPropertyTree(), i);
        i = GetNextItem(i, TVGN_NEXT);
    }       
}

void CInspectorTreeCtrl::OnItemExpanded(NMHDR* pNMHDR, LRESULT* pResult) 
{
    NM_TREEVIEW * pNMTreeView = reinterpret_cast <NM_TREEVIEW*> (pNMHDR);   
    dynExpand(pNMTreeView->itemNew.hItem);
    *pResult = 0;
}


void CInspectorTreeCtrl::AddLevel(IPropertyTree & pTree, HTREEITEM hParent)
{
    CTreeListItem * itm = GetTreeListItem(hParent);
    if(!itm->isVisible())
    {
        CString txt;
        TV_INSERTSTRUCT is;
        is.hInsertAfter = TVI_LAST;
        is.item.mask = TVIF_TEXT | TVIF_PARAM;
        is.item.pszText = LPSTR_TEXTCALLBACK;       
        // place holder for children
        if (pTree.hasChildren())
        {
            is.hParent = hParent;
            is.item.lParam = reinterpret_cast <DWORD> (createTreeListAttribute("@[loading...]", pTree));
            HTREEITEM thisTreeItem = InsertItem(&is);
            ASSERT(thisTreeItem != NULL);
        }
        itm->setVisible();
    }
}


void CInspectorTreeCtrl::BeginFind()
{
}


void CInspectorTreeCtrl::EndFind()
{
    if(finder)
    {
        finder->kill();
        finder->join();
        delete finder;
        finder = NULL;
    }
}


void CInspectorTreeCtrl::NextFind(LPCSTR txt, BOOL matchCase, BOOL wholeWord)
{
    if(!finder)
    {
        finder = new CFinderThread(*this, txt, matchCase, wholeWord);
        finder->start();
    }
    else
    {
        finder->next();
    }
}


void CInspectorTreeCtrl::OnSetAsRoot() 
{
    HTREEITEM hItem = GetSelectedItem();
    if(hItem)
    {
        CString xp;
        xpath(hItem, xp);
        NewTree(xp);
    }
}


bool CInspectorTreeCtrl::GetNewItem(NewValue_t nvt, CString & name, CString & value, HTREEITEM & hParent)
{
    hParent = GetSelectedItem();
    if(hParent)
    {
        Invalidate();
        CNewValueDlg nvDlg(nvt, name, value, this);
        if(nvDlg.DoModal() == IDOK)
        {
            name = nvDlg.GetName();
            value = nvDlg.GetValue();
            return true;
        }
    }   
    return false;
}

void CInspectorTreeCtrl::OnAddAttribute() 
{
    CString name, value;
    HTREEITEM hParent;
    while(GetNewItem(NVT_attribute, name, value, hParent))
    {
        if(connection->lockWrite())
        {
            CString attrName;   
            
            if(name[0] != '@')
            {
                attrName = "@";
                attrName += name;
            }
            else
                attrName = name;

            IPropertyTree * pTree = GetTreeListItem(hParent)->queryPropertyTree();
            pTree->addProp(attrName, value);

            TV_INSERTSTRUCT is;
            is.hInsertAfter = TVI_LAST;
            is.item.mask = TVIF_TEXT | TVIF_PARAM;
            is.item.pszText = LPSTR_TEXTCALLBACK;           
            is.hParent = hParent;
            is.item.lParam = reinterpret_cast <DWORD> (createTreeListAttribute(attrName, *pTree));
            InsertItem(&is);

            connection->unlockWrite();
            Expand(hParent, TVE_EXPAND);            
            break;      
        }
    }
}


void CInspectorTreeCtrl::OnAddProperty() 
{
    HTREEITEM hParent;
    CString name, value;

    while(GetNewItem(NVT_property, name, value, hParent))
    {
        if(connection->lockWrite())
        {
            IPropertyTree * t = createPTree();
            t->setProp(NULL, value);
            t = GetTreeListItem(hParent)->queryPropertyTree()->addPropTree(name, t);    

            TV_INSERTSTRUCT is;
            is.hInsertAfter = TVI_LAST;
            is.item.mask = TVIF_TEXT | TVIF_PARAM;
            is.item.pszText = LPSTR_TEXTCALLBACK;           
            is.hParent = hParent;
            is.item.lParam = reinterpret_cast <DWORD> (createTreeListProperty(t->queryName(), * t));
            InsertItem(&is);

            connection->unlockWrite();
            Expand(hParent, TVE_EXPAND);            
            break;
        }
        else
            MessageBox("Unable to lock connection for write", "Cannot Obtain Lock", MB_OK); 
    }
}


void CInspectorTreeCtrl::OnDelete() 
{
    DeleteCurrentItem();
}

void CInspectorTreeCtrl::OnHScroll(UINT nSBCode, UINT nPos, CScrollBar* pScrollBar) 
{
    /* NULL */ 
}



// ----- Inspector frame window --------------------------------------------------


BEGIN_MESSAGE_MAP(CPropertyInspector, CWnd)
    ON_WM_SIZE()
END_MESSAGE_MAP()



CPropertyInspector::~CPropertyInspector()
{
    connection = NULL;
}

LONG FAR PASCAL CPropertyInspector::wndProc(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    return ::DefWindowProc(hWnd, msg, wParam, lParam);
}

LRESULT CPropertyInspector::WindowProc(UINT Msg, WPARAM wParam, LPARAM lParam)
{
    switch(Msg)
    {
    case MSG_COLUMN_SIZED:  
        CRect rect;
        GetWindowRect(&rect);
        setColumnWidth(1, rect.Width() - getColumnWidth(0) - 2);
        inspectorCtrl.Invalidate();
        if(inspectorCtrl.EIPActive) inspectorCtrl.PostMessage(MSG_EIP_RESIZE);
        return 0;
    }
    return CWnd::WindowProc(Msg, wParam, lParam);
}

void CPropertyInspector::registerClass()
{
    WNDCLASS wc;
    memset(&wc, 0, sizeof(wc));
    
    wc.style = CS_DBLCLKS | CS_VREDRAW | CS_HREDRAW | CS_GLOBALCLASS;
    wc.lpfnWndProc = (WNDPROC)wndProc;
    wc.hInstance = AfxGetInstanceHandle();
    wc.hCursor = 0;
    wc.lpszClassName = "PROPERTY_INSPECTOR_CTRL";
    wc.hbrBackground = (HBRUSH) GetStockObject(LTGRAY_BRUSH);
    
    if (!::RegisterClass(&wc)) ASSERT(FALSE);   
}

BOOL CPropertyInspector::SubclassDlgItem(UINT id, CWnd * parent)
{
    return CWnd::SubclassDlgItem(id, parent) ? initialize() : FALSE;
}

CWnd * CPropertyInspector::SetFocus()
{
    return inspectorCtrl.SetFocus();
}

int CPropertyInspector::initialize()
{
    CRect rect; 
    GetWindowRect(rect);

    if(!staticCtrl.Create(NULL, WS_CHILD | WS_VISIBLE | SS_SUNKEN, CRect(0, 0 ,0 ,0), this)) return FALSE;
    staticCtrl.SetWindowPos(&wndBottom, 0, 0, rect.Width(), rect.Height(), SWP_SHOWWINDOW);

    if(!headerCtrl.Create(WS_CHILD | WS_VISIBLE |  HDS_HORZ, CRect(0, 0, 0, 0), this, IDC_TREE_LIST_HEADER)) return FALSE;
    CSize textSize;         
    headerCtrl.SetFont(GetParent()->GetFont());
    CDC * dc = headerCtrl.GetDC();
    textSize = dc->GetTextExtent("A");
    headerCtrl.ReleaseDC(dc);
    headerCtrl.SetWindowPos(NULL, 1, 1, rect.Width() - 2, textSize.cy + 4, SWP_SHOWWINDOW | SWP_NOZORDER);

    HD_ITEM hdItem;
    hdItem.mask = HDI_FORMAT | HDI_TEXT | HDI_WIDTH;
    hdItem.fmt = HDF_LEFT | HDF_STRING;
    hdItem.pszText = "Value";
    hdItem.cchTextMax = 5;
    hdItem.cxy = 140;
    headerCtrl.InsertItem(0, &hdItem);
    hdItem.pszText = "Property";
    hdItem.cchTextMax = 8;
    hdItem.cxy = rect.Width() - hdItem.cxy;
    headerCtrl.InsertItem(0, &hdItem);

    if(!inspectorCtrl.Create(WS_CHILD | WS_VISIBLE | TVS_SHOWSELALWAYS | TVS_HASLINES |TVS_LINESATROOT | TVS_HASBUTTONS |TVS_DISABLEDRAGDROP, CRect(0, 0, 0, 0), this, IDC_TREE_LIST_CTRL)) return FALSE;
    inspectorCtrl.SetWindowPos(NULL, 1, textSize.cy + 5, rect.Width(), rect.Height() - (textSize.cy + 6), SWP_SHOWWINDOW | SWP_NOZORDER);
    PostMessage(MSG_COLUMN_SIZED);

    return TRUE;
}

BOOL CPropertyInspector::OnNotify(WPARAM wParam, LPARAM lParam, LRESULT * result)
{   
    LPNMHEADER hdn = reinterpret_cast <LPNMHEADER> (lParam);

    if(wParam == IDC_TREE_LIST_HEADER)
    {   
        if(hdn->hdr.code == HDN_ENDTRACK) PostMessage(MSG_COLUMN_SIZED);        
    }

    return CWnd::OnNotify(wParam, lParam, result);
}

void CPropertyInspector::OnSize(UINT type, int cx, int cy)
{
    CWnd::OnSize(type, cx, cy);
    staticCtrl.MoveWindow(0, 0, cx, cy);
    CRect headerRect;
    headerCtrl.GetWindowRect(&headerRect);
    headerCtrl.MoveWindow(1, 1, cx - 2, headerRect.Height());   
    inspectorCtrl.MoveWindow(1, headerRect.Height() + 1, cx - 2, cy - headerRect.Height() - 2);
    setColumnWidth(1, cx - getColumnWidth(0) - 2);
}

void CPropertyInspector::NewTree(IConnection * conn)
{
    connection = conn;
    if(connection) inspectorCtrl.NewTree();
}


void CPropertyInspector::KillTree()
{
    connection = NULL;
    inspectorCtrl.DeleteAllItems();
}

UINT CPropertyInspector::GetCount()
{
    return inspectorCtrl.GetCount();
}

void CPropertyInspector::NextFind(LPCSTR txt, BOOL MatchCase, BOOL MatchWholeWord)
{
    inspectorCtrl.NextFind(txt, MatchCase, MatchWholeWord);
}


void CPropertyInspector::BeginFind()
{
    inspectorCtrl.BeginFind();
}


void CPropertyInspector::EndFind()
{
    inspectorCtrl.EndFind();
}


int CPropertyInspector::getColumnWidth(int idx)
{
    if(idx < 2)
    {
        static HD_ITEM hdItem;
        hdItem.mask = HDI_WIDTH;
        if(headerCtrl.GetItem(idx, &hdItem)) return hdItem.cxy;     
    }
    return 0;
}

void CPropertyInspector::setColumnWidth(int idx, int wid)
{
    if(idx < 2)
    {
        static HD_ITEM hdItem;
        hdItem.mask = HDI_WIDTH;
        hdItem.cxy = wid;
        headerCtrl.SetItem(idx, &hdItem);
    }
}

void CPropertyInspector::showAttribs(bool show)
{
    if(ShowAttrsOnProps != show)
    {
        ShowAttrsOnProps = show;
        inspectorCtrl.Invalidate();
    }
}

void CPropertyInspector::showQualified(bool show)
{
    if(ShowQualifiedNames != show)
    {
        ShowQualifiedNames = show;
        inspectorCtrl.Invalidate();
    }
}





/////////////////////////////////////////////////////////////////////////////
// CEditEIP

CEditEIP::CEditEIP()
{
    tli = NULL;
}

CEditEIP::~CEditEIP()
{
}


BEGIN_MESSAGE_MAP(CEditEIP, CEdit)
    //{{AFX_MSG_MAP(CEditEIP)
    ON_WM_CREATE()
    ON_WM_KEYUP()
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CEditEIP message handlers


int CEditEIP::OnCreate(LPCREATESTRUCT lpCreateStruct) 
{
    if(CEdit::OnCreate(lpCreateStruct) == -1) return -1;
    parent = static_cast <CInspectorTreeCtrl *> (GetParent());
    ASSERT(parent != NULL);
    SetFont(parent->GetFont());
    return 0;
}

BOOL CEditEIP::Activate(CTreeListItem * i, CRect & rect)
{
    ASSERT(i != NULL);
    if(!i->isBinary())
    {
        tli = i;
        Resize(rect);       
        SetFocusText(tli->getValue());
        GetWindowText(ValuePreserve);
        return TRUE;        
    }
    return FALSE;
}

BOOL CEditEIP::Deactivate(BOOL save)
{
    BOOL r = FALSE;
    if(IsActive())
    {
        ASSERT(!tli->isBinary());

        ShowWindow(FALSE);
        CString ecTxt;
        GetWindowText(ecTxt);           

        if(connection && save && GetModify())
        {
            if((!tli->getValue() || strcmp(tli->getValue(), ecTxt) != 0))
            {   
                if(tli->setValue(ecTxt)) 
                {
                    GetParent()->Invalidate();
                    r = TRUE;
                }
                else
                {   
                    ::MessageBox(NULL, "Unable to gain exclusive lock for write", "Unable to lock", MB_OK);
                }
            }   
        }
        tli = NULL;
    }
    return r;
}

BOOL CEditEIP::IsActive()
{
    return tli == NULL ? FALSE : TRUE;
}


void CEditEIP::OnKeyUp(UINT nChar, UINT nRepCnt, UINT nFlags) 
{
    switch(nChar)
    {
    case 0x09:              // tab key
    case 0x0d:              // enter key - accept value
        Deactivate();
        break;
    case 0x1b:              // esc key - get value when editing began
        SetFocusText(ValuePreserve);        
        break;
    default:
        CEdit::OnKeyUp(nChar, nRepCnt, nFlags);
        break;
    }
}

void CEditEIP::SetFocusText(LPCSTR text)
{
    SetWindowText(text);
    SetModify(FALSE);
    SetFocus();
    SetSel(0, -1);      // NB select all
}

BOOL CEditEIP::Resize(CRect & rect)
{
    return IsActive() ? SetWindowPos(&wndTop, rect.left, rect.top, rect.Width(), rect.Height(), SWP_SHOWWINDOW) : FALSE;
}




