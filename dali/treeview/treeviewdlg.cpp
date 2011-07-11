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

// treeviewDlg.cpp : implementation file
//



#include "stdafx.h"

#include "treeview.h"
#include "treeviewDlg.h"
#include "util.hpp"

#include "jptree.hpp"
#include "jfile.hpp"
#include "jlib.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "daclient.hpp"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif


#define DEFAULT_FILE_EXT        "xml"
#define DEFAULT_FILE_MASK       "*.xml"
#define DEFAULT_TITLE           "Property Tree Inspector"

#define CONNECT_SECTION         "RemoteConnect"
#define COVEN_SIZE              "CovenSize"

    
UINT WM_FINDREPLACE = ::RegisterWindowMessage(FINDMSGSTRING);



// ROYMORE may be better to move these to a local Property Tree



class CAboutDlg : public CDialog
{
public:
    CAboutDlg();

// Dialog Data
    //{{AFX_DATA(CAboutDlg)
    enum { IDD = IDD_ABOUTBOX };
    //}}AFX_DATA

    // ClassWizard generated virtual function overrides
    //{{AFX_VIRTUAL(CAboutDlg)
    protected:
    virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
    //}}AFX_VIRTUAL

// Implementation
protected:
    //{{AFX_MSG(CAboutDlg)
    //}}AFX_MSG
    DECLARE_MESSAGE_MAP()
};

CAboutDlg::CAboutDlg() : CDialog(CAboutDlg::IDD)
{
    //{{AFX_DATA_INIT(CAboutDlg)
    //}}AFX_DATA_INIT
}

void CAboutDlg::DoDataExchange(CDataExchange* pDX)
{
    CDialog::DoDataExchange(pDX);
    //{{AFX_DATA_MAP(CAboutDlg)
    //}}AFX_DATA_MAP
}

BEGIN_MESSAGE_MAP(CAboutDlg, CDialog)
    //{{AFX_MSG_MAP(CAboutDlg)
        // No message handlers
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()






class CExpanderThread : public Thread
{
private:
    CTreeviewDlg * dlg;
    CInspectorTreeCtrl * tree;
    bool expand;

public:
    CExpanderThread(CTreeviewDlg * _dlg, bool _expand) 
    {
        dlg = _dlg;
        tree = dlg->inspector.getTree();
        expand = _expand;   
    }

    void process(HTREEITEM in, int lvl)
    {       
        if(lvl > 0)
        {
            if(expand)
            {
                if(tree->ItemHasChildren(in) && !(tree->GetItemState(in, TVIS_EXPANDED) & TVIS_EXPANDED)) 
                {
                    tree->Expand(in, TVE_EXPAND);
                    lvl--;
                }
            }
            else if(tree->ItemHasChildren(in) && tree->GetItemState(in, TVIS_EXPANDED) & TVIS_EXPANDED)
                tree->Expand(in, TVE_COLLAPSE);             
            
            HTREEITEM i = tree->GetChildItem(in);       
            while(i)
            {
                process(i, lvl);
                i = tree->GetNextItem(i, TVGN_NEXT);
            }           
        }
    }

    virtual int run()
    {       
        process(tree->GetRootItem(), 1);
        dlg->endExpand();
        return 0;
    }
};





BEGIN_MESSAGE_MAP(CTreeviewDlg, CDialog)
    //{{AFX_MSG_MAP(CTreeviewDlg)
    ON_WM_SYSCOMMAND()
    ON_WM_PAINT()
    ON_WM_QUERYDRAGICON()
    ON_WM_SIZE()
    ON_COMMAND_EX(IDD_NEXT_FIELD, OnNextPrevField)
    ON_WM_CLOSE()
    ON_BN_CLICKED(IDC_EXIT, OnExit)
    ON_COMMAND(IDM_LOAD, OnMenuLoad)
    ON_COMMAND(IDM_SAVE, OnMenuSave)
    ON_COMMAND(IDM_EXIT, OnMenuExit)
    ON_COMMAND(IDM_EXPANDALL, OnMenuExpandall)
    ON_COMMAND(IDM_CONTRACTALL, OnMenuContractall)
    ON_COMMAND(IDM_FIND, OnMenuFind)
    ON_COMMAND(IDM_DELETE, OnMenuDelete)
    ON_COMMAND(IDM_DELETECONFIRM, OnMenuDeleteConfirm)
    ON_COMMAND(IDM_ABOUT, OnMenuAbout)
    ON_COMMAND(IDM_SHOWATTRIBS, OnMenuShowAttribs)
    ON_COMMAND(IDM_SHOWQUALIFIED, OnMenuShowQualified)
    ON_COMMAND(IDM_CONNECTREMOTE, OnMenuConnectRemote)
    ON_COMMAND(IDM_RECONNECT, OnMenuReconnect)
    ON_COMMAND(IDM_COMMIT, OnCommit)
    ON_WM_CREATE()
    ON_COMMAND_EX(IDD_PREV_FIELD, OnNextPrevField)
    //}}AFX_MSG_MAP
    ON_REGISTERED_MESSAGE(WM_FINDREPLACE, OnFindReplace)
END_MESSAGE_MAP()


CTreeviewDlg::CTreeviewDlg(LPCSTR fn, CWnd* pParent) : CDialog(CTreeviewDlg::IDD, pParent)
{
    //{{AFX_DATA_INIT(CTreeviewDlg)
        // NOTE: the ClassWizard will add member initialization here
    //}}AFX_DATA_INIT
    // Note that LoadIcon does not require a subsequent DestroyIcon in Win32
    m_hIcon = AfxGetApp()->LoadIcon(IDR_MAINFRAME);
    tooltipCtrl = NULL;
    hAccel = NULL;
    findReplaceDialog = NULL;
    expander = NULL;
    connection = NULL;
    cmdfname = fn ? strdup(fn) : NULL;
}


CTreeviewDlg::~CTreeviewDlg()
{
    free(cmdfname);
    if(expander) delete expander;
    delete tooltipCtrl;
}


void CTreeviewDlg::setWindowTitle()
{
    CString title(DEFAULT_TITLE);
    if(connection && connection->queryName())
    {
        title += " [";
        title += connection->queryName();
        title += "]";
    }
    SetWindowText(title);
}

void CTreeviewDlg::resetDialog()
{
    inspector.KillTree();
    if(connection)
    {
        connection->Release();
        connection = NULL;
    }
    setWindowTitle();
    updateMenuState();
}

void CTreeviewDlg::DoDataExchange(CDataExchange* pDX)
{
    CDialog::DoDataExchange(pDX);
    //{{AFX_DATA_MAP(CTreeviewDlg)
        // NOTE: the ClassWizard will add DDX and DDV calls here
    //}}AFX_DATA_MAP
}


BOOL CTreeviewDlg::OnInitDialog()
{
    CDialog::OnInitDialog();

    hAccel = LoadAccelerators(AfxGetInstanceHandle(), MAKEINTRESOURCE(IDR_ACCELERATOR1));

    SetIcon(m_hIcon, TRUE);         // Set big icon
    SetIcon(m_hIcon, FALSE);        // Set small icon

    inspector.SubclassDlgItem(IDC_INSPECTOR, this);

    tooltipCtrl = new CToolTipCtrl;
    tooltipCtrl->Create(this);
    
    CRect DialogRect, rect;
    GetWindowRect(DialogRect);
            
    CWnd * wnd = GetDlgItemRect(IDC_EXIT, rect);
    CloseRightOffset = DialogRect.right - rect.left;
    CloseBottomOffset = DialogRect.bottom - rect.top;
    tooltipCtrl->AddTool(wnd, "Close the property inspector");

    wnd = GetDlgItemRect(IDC_INSPECTOR, rect);
    TreeRightMargin = DialogRect.right - rect.right;
    TreeBottomMargin = DialogRect.bottom - rect.bottom;

    if(cmdfname)
        _loadTree(cmdfname);
    else
        updateMenuState();
    
    return FALSE; 
}

void CTreeviewDlg::OnSysCommand(UINT nID, LPARAM lParam)
{
    if ((nID & 0xFFF0) == IDM_ABOUTBOX)
    {
        CAboutDlg dlgAbout;
        dlgAbout.DoModal();
    }
    else
    {
        CDialog::OnSysCommand(nID, lParam);
    }
}


BOOL CTreeviewDlg::PreTranslateMessage(MSG * msg) 
{
    if(tooltipCtrl) tooltipCtrl->RelayEvent(msg);
    
    if(msg->message >= WM_KEYFIRST && msg->message <= WM_KEYLAST)
        return ::TranslateAccelerator(m_hWnd, hAccel, msg); 
            
    return CDialog::PreTranslateMessage(msg);
}


void CTreeviewDlg::OnPaint() 
{
    if (IsIconic())
    {
        CPaintDC dc(this);
        CRect rect;
        GetClientRect(&rect);

        SetWindowText(DEFAULT_TITLE);
        SendMessage(WM_ICONERASEBKGND, (WPARAM) dc.GetSafeHdc(), 0);
        int x = (rect.Width() - GetSystemMetrics(SM_CXICON) + 1) / 2;
        int y = (rect.Height() - GetSystemMetrics(SM_CYICON) + 1) / 2;
        dc.DrawIcon(x, y, m_hIcon);
    }
    else
    {
        CDialog::OnPaint();
    }
}


void CTreeviewDlg::OnCommit()
{
    if(connection) connection->commit();
}


HCURSOR CTreeviewDlg::OnQueryDragIcon()
{
    return (HCURSOR) m_hIcon;
}


void CTreeviewDlg::OnSize(UINT nType, int cx, int cy) 
{
    CDialog::OnSize(nType, cx, cy);
    
    if(!IsIconic())
    {
        CRect rect, DialogRect;
        GetWindowRect(DialogRect);

        HDWP hDefer = BeginDeferWindowPos(16);

        CWnd * Wnd = GetDlgItemRect(IDC_EXIT, rect);
        if(Wnd)
        {
            int width = rect.Width();
            int height = rect.Height();
            rect.top = DialogRect.bottom - CloseBottomOffset;
            rect.bottom = rect.top + height;
            rect.left = DialogRect.right - CloseRightOffset;
            rect.right = rect.left + width;
            ScreenToClient(&rect);
            DeferWindowPos(hDefer, Wnd->m_hWnd, NULL, rect.left, rect.top, rect.Width(), rect.Height(), SWP_NOZORDER | SWP_NOREDRAW);
        }
        Wnd = GetDlgItemRect(IDC_INSPECTOR, rect);
        if(Wnd)
        {
            rect.right = DialogRect.right - TreeRightMargin;
            rect.bottom = DialogRect.bottom - TreeBottomMargin;
            ScreenToClient(&rect);
            DeferWindowPos(hDefer, Wnd->m_hWnd, NULL, rect.left, rect.top, rect.Width(), rect.Height(), SWP_NOZORDER | SWP_NOREDRAW);                   
        }

        EndDeferWindowPos(hDefer);
        Invalidate();
    }   
}

void CTreeviewDlg::OnMenuSave() 
{
    if(connection && connection->getType() != CT_none)
    {
        CFileDialog fileDialog(FALSE, DEFAULT_FILE_EXT, DEFAULT_FILE_MASK, OFN_HIDEREADONLY | OFN_NOCHANGEDIR, "XML Files (*.xml)|*.xml||", this);

        while(fileDialog.DoModal() != IDCANCEL)
        {
            CString fullfname = fileDialog.GetPathName();
            if(!fullfname.IsEmpty())
            {
                if(fileDialog.GetFileExt().IsEmpty()) fullfname += ".xml";          
                if(_saveTree(fullfname)) break;             
            }
        }
    }   
    setWindowTitle();
}

bool CTreeviewDlg::_loadTree(LPCSTR fname)
{
    resetDialog();
    connection = createLocalConnection(fname);
    if(connection->getType() == CT_local)
    {
        inspector.NewTree(connection);
        setWindowTitle();
        updateMenuState();
        inspector.SetFocus();
        inspector.Invalidate();
        return true;        
    }
    MessageBox("Tree failed to load", "Load Failure", MB_OK | MB_ICONEXCLAMATION);
    return false;
}


bool CTreeviewDlg::_saveTree(LPCSTR fname)
{
    return connection && connection->getType() != CT_none && saveTree(fname, *connection->queryRoot()) ? true : false;  
}


void CTreeviewDlg::OnMenuLoad() 
{
    CFileDialog fileDialog(TRUE, DEFAULT_FILE_EXT, DEFAULT_FILE_MASK, OFN_HIDEREADONLY | OFN_FILEMUSTEXIST | OFN_NOCHANGEDIR, "XML Files (*.xml)|*.xml||", this);

    while(fileDialog.DoModal() != IDCANCEL)
    {
        resetDialog();
        CString fullfname = fileDialog.GetPathName();
        if(!fullfname.IsEmpty() && _loadTree(fullfname)) break;     
    }
}


BOOL CTreeviewDlg::OnNextPrevField(UINT cmdId)
{
    static const UINT ctrls[] = {IDC_TREE_LIST_CTRL, IDC_EXIT};
    static const int ctrlsMax = sizeof(ctrls) / sizeof(UINT);

    UINT id = GetFocus()->GetDlgCtrlID();
    for(int i = 0; i < ctrlsMax; i++)
    {
        if(ctrls[i] == id)
        {
            if(cmdId == IDD_NEXT_FIELD)
                i = (++i % ctrlsMax);
            else
                i = i ? --i : ctrlsMax - 1; 

            switch(ctrls[i])
            {
            case IDC_TREE_LIST_CTRL:
                inspector.SetFocus();
                return TRUE;
            default:
                GotoDlgCtrl(GetDlgItem(ctrls[i]));
                return TRUE;
            }
        }
    }
    return FALSE;
}


void CTreeviewDlg::OnMenuAbout() 
{
    CAboutDlg about;
    about.DoModal();
}


void CTreeviewDlg::OnMenuShowAttribs()
{
    CMenu * menu = GetMenu();
    ASSERT(menu != NULL);
    if(menu->GetMenuState(IDM_SHOWATTRIBS, MF_BYCOMMAND) & MF_CHECKED)
    {
        menu->CheckMenuItem(IDM_SHOWATTRIBS, MF_BYCOMMAND | MF_UNCHECKED);
        inspector.showAttribs(false);
    }
    else
    {
        menu->CheckMenuItem(IDM_SHOWATTRIBS, MF_BYCOMMAND | MF_CHECKED);
        inspector.showAttribs();
    }
}

void CTreeviewDlg::OnMenuShowQualified()
{
    CMenu * menu = GetMenu();
    ASSERT(menu != NULL);
    if(menu->GetMenuState(IDM_SHOWQUALIFIED, MF_BYCOMMAND) & MF_CHECKED)
    {
        menu->CheckMenuItem(IDM_SHOWQUALIFIED, MF_BYCOMMAND | MF_UNCHECKED);
        inspector.showQualified(false);
    }
    else
    {
        menu->CheckMenuItem(IDM_SHOWQUALIFIED, MF_BYCOMMAND | MF_CHECKED);
        inspector.showQualified();
    }

}

void CTreeviewDlg::OnMenuDelete() 
{
    inspector.getTree()->DeleteCurrentItem(false);
}

void CTreeviewDlg::OnMenuDeleteConfirm() 
{
    inspector.getTree()->DeleteCurrentItem();
}

void CTreeviewDlg::OnMenuFind() 
{
    findReplaceDialog = new CFindReplaceDialog();                       // deleted by itself
    if(!findReplaceDialog->Create(TRUE, findStr, NULL, FR_DOWN, this))
    {
        delete findReplaceDialog;
        findReplaceDialog = NULL;
    }
    updateMenuState();
    inspector.BeginFind();
}

LRESULT CTreeviewDlg::OnFindReplace(WPARAM wParam, LPARAM lParam)
{
    ASSERT(findReplaceDialog != NULL);

    if(findReplaceDialog->IsTerminating())
    {
        inspector.EndFind();
        findReplaceDialog = NULL;       // will delete itself
        updateMenuState();
    }
    else if(findReplaceDialog->FindNext())
    {
        inspector.NextFind(findReplaceDialog->GetFindString(), findReplaceDialog->MatchCase(), findReplaceDialog->MatchWholeWord());
    }
    return 0;
}


void CTreeviewDlg::OnMenuExpandall() 
{
    if(expander) delete expander;   
    expander = new CExpanderThread(this, true);
    updateMenuState();
    expander->start();
}


void CTreeviewDlg::OnMenuContractall() 
{
    if(expander) delete expander;   
    expander = new CExpanderThread(this, false);
    updateMenuState();
    expander->start();
}


void CTreeviewDlg::endExpand()
{
    expander->Release();
    expander = NULL;
    updateMenuState();
}


void CTreeviewDlg::OnMenuExit() 
{
    OnExit();   
}

void CTreeviewDlg::OnMenuConnectRemote() 
{
    resetDialog();

    CConnectDlg ConnectDlg(this);
    if(ConnectDlg.DoModal() == IDOK)
    {
        connectedEpa.kill();
        for(int i = 0; i < ConnectDlg.GetCovenSize(); i++)
        {
            SocketEndpoint ep;
            toEp(ep, ConnectDlg.GetServerEndpoint(i));
            connectedEpa.append(ep);            
        }

        connection = createRemoteConnection(connectedEpa);
        if(connection->getType() == CT_remote)
        {
            inspector.NewTree(connection);
            setWindowTitle();
            updateMenuState();
        }
        else
            MessageBox("Failed to connect to any of the remote Dali servers\nthat you listed in the connection details.", "Connection Failed", MB_OK | MB_ICONEXCLAMATION);
    } 
}

void CTreeviewDlg::OnMenuReconnect() 
{
    if (!connectedEpa.ordinality())
    {
        OnMenuConnectRemote();
        return;
    }
    resetDialog();

    connection = createRemoteConnection(connectedEpa);
    if(connection->getType() == CT_remote)
    {
        inspector.NewTree(connection);
        setWindowTitle();
        updateMenuState();
    }
    else
        MessageBox("Failed to reconnect to any of the remote Dali servers\nthat you listed in the connection details.", "Connection Failed", MB_OK | MB_ICONEXCLAMATION);
}

void CTreeviewDlg::OnExit() 
{
    resetDialog();
    EndDialog(0);
}


void CTreeviewDlg::OnClose() 
{   
    resetDialog();
    CDialog::OnClose();
}


CWnd * CTreeviewDlg::GetDlgItemRect(int nId, RECT & rect)
{
    CWnd * r = GetDlgItem(nId);
    if(r) r->GetWindowRect(&rect);
    return r;
}


#define MENUENABLE(i) menu->EnableMenuItem(i, MF_ENABLED)
#define MENUDISABLE(i) menu->EnableMenuItem(i, MF_GRAYED)

void CTreeviewDlg::updateMenuState()
{
    CMenu * menu = GetMenu();
    ASSERT(menu != NULL);

    if(findReplaceDialog || expander)
    {
        MENUDISABLE(IDM_LOAD);
        MENUDISABLE(IDM_SAVE);
        MENUDISABLE(IDM_CONNECTREMOTE);
        MENUDISABLE(IDM_RECONNECT);
        MENUDISABLE(IDM_FIND);
        MENUDISABLE(IDM_XFIND);
        MENUDISABLE(IDM_COMMIT);
        MENUDISABLE(IDM_REFRESH);
        MENUDISABLE(IDM_SHOWATTRIBS);
        MENUDISABLE(IDM_SHOWQUALIFIED);
        MENUDISABLE(IDM_CONTRACTALL);
        MENUDISABLE(IDM_EXPANDALL);
    }
    else
    {
        MENUDISABLE(IDM_XFIND);
        if(connection && connection->getType() != CT_none)
        {
            MENUENABLE(IDM_FIND);
            MENUENABLE(IDM_CONTRACTALL);
            MENUENABLE(IDM_EXPANDALL);
            MENUENABLE(IDM_SAVE);
            MENUENABLE(IDM_CONNECTREMOTE);
            MENUENABLE(IDM_RECONNECT);
            MENUENABLE(IDM_REFRESH);
            MENUENABLE(IDM_SHOWATTRIBS);
            MENUENABLE(IDM_SHOWQUALIFIED);
            if(connection->getType() == CT_remote)
                MENUENABLE(IDM_COMMIT);
            else
                MENUDISABLE(IDM_COMMIT);
        }
        else
        {
            MENUDISABLE(IDM_FIND);
            MENUDISABLE(IDM_CONTRACTALL);
            MENUDISABLE(IDM_EXPANDALL);
            MENUDISABLE(IDM_SAVE);
            MENUDISABLE(IDM_REFRESH);
            MENUDISABLE(IDM_SHOWATTRIBS);
            MENUDISABLE(IDM_SHOWQUALIFIED);
            MENUDISABLE(IDM_COMMIT);
        }
    }
}










bool saveTree(LPCSTR fname, IPropertyTree & pTree)
{
    bool r = true;      // let's be optimistic
    StringBuffer xml;
    toXML(&pTree, xml);

    IFile * f = createIFile(fname);
    if(f)
    {
        IFileIO * io = f->open(IFOcreate);
        if(io)
        {
            if(io->write(0, xml.length(), xml.toCharArray()) != xml.length())
            {
                showFIOErr(fname, false);
                r = false;
            }
            io->Release();
        }
        else
        {
            showFIOErr(fname, false);
            r = false;
        }
        f->Release();
    }
    else
    {
        showFIOErr(fname, false);
        r = false;
    }   
    return r;
}



/////////////////////////////////////////////////////////////////////////////
// CConnectDlg dialog


CConnectDlg::CConnectDlg(CWnd* pParent) : CDialog(CConnectDlg::IDD, pParent)
{
    //{{AFX_DATA_INIT(CConnectDlg)
        // NOTE: the ClassWizard will add member initialization here
    //}}AFX_DATA_INIT
}


CConnectDlg::~CConnectDlg()
{
    killEndpoints();
}


void CConnectDlg::DoDataExchange(CDataExchange* pDX)
{
    CDialog::DoDataExchange(pDX);
    //{{AFX_DATA_MAP(CConnectDlg)
        // NOTE: the ClassWizard will add DDX and DDV calls here
    //}}AFX_DATA_MAP
}


BEGIN_MESSAGE_MAP(CConnectDlg, CDialog)
    //{{AFX_MSG_MAP(CConnectDlg)
    ON_BN_CLICKED(IDC_REMOVESERVER_BUTTON, OnRemoveServerButton)
    ON_BN_CLICKED(IDC_ADDSERVER_BUTTON, OnAddServerButton)
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CConnectDlg message handlers


void CConnectDlg::killEndpoints()
{
    for(int i = 0; i < ServerEndpoints.GetSize(); i++) delete ServerEndpoints.GetAt(i);
    ServerEndpoints.RemoveAll();
}   

LPCSTR CConnectDlg::GetServerEndpoint(int idx)
{
    CString & t = *ServerEndpoints.GetAt(idx);
    return idx < ServerEndpoints.GetSize() ? (LPCSTR)t : NULL;
}


int CConnectDlg::GetCovenSize()
{
    return CovenSize;
}

int CConnectDlg::GetDlgItemInt(UINT id)
{
    CString tmp;
    GetDlgItem(id)->GetWindowText(tmp);
    return atoi(tmp);
}

void CConnectDlg::OnOK() 
{
    CListBox * listBox = static_cast <CListBox *> (GetDlgItem(IDC_SERVERS_LIST));
    CovenSize = listBox->GetCount();
    if(CovenSize > 0)
    {
        putProfile(CONNECT_SECTION, COVEN_SIZE, CovenSize);
        killEndpoints();
        for(int i = 0; i < CovenSize; i++)
        {
            CString * ep;
            char key[32] = "ServerEP_";
            itoa(i, &key[9], 10);
            ep = new CString();
            listBox->GetText(i, *ep);
            ServerEndpoints.Add(ep);
            putProfile(CONNECT_SECTION, key, *ep);
        }

        CDialog::OnOK();            
    }
    else    
        MessageBox("There must be at least 1 server in\nthe coven.", "Connection Configuration Error", MB_OK | MB_ICONEXCLAMATION); 
}

void CConnectDlg::OnCancel() 
{
    CDialog::OnCancel();
}

BOOL CConnectDlg::OnInitDialog() 
{
    CDialog::OnInitDialog();
    
    CListBox * listBox = static_cast <CListBox *> (GetDlgItem(IDC_SERVERS_LIST));
    for(int i = 0; i < getProfileInt(CONNECT_SECTION, COVEN_SIZE); i++)
    {
        char key[32] = "ServerEP_";
        itoa(i, &key[9], 10);
        listBox->AddString(getProfileStr(CONNECT_SECTION, key));
    }

    CovenSize = 0;  
    return TRUE; 
}

void CConnectDlg::OnRemoveServerButton() 
{
    CListBox * lb = static_cast <CListBox *> (GetDlgItem(IDC_SERVERS_LIST));
    if(lb->GetCurSel() >= 0) lb->DeleteString(lb->GetCurSel());     
}

void CConnectDlg::OnAddServerButton() 
{
    CEndpointDlg epd;
    
    if(epd.DoModal() == IDOK)
    {
        CListBox * lb = static_cast <CListBox *> (GetDlgItem(IDC_SERVERS_LIST));
        lb->AddString(epd.GetEndpointStr());
    }
}





/////////////////////////////////////////////////////////////////////////////
// CEndpointDlg dialog


CEndpointDlg::CEndpointDlg(CWnd* pParent) : CDialog(CEndpointDlg::IDD, pParent)
{
    //{{AFX_DATA_INIT(CEndpointDlg)
        // NOTE: the ClassWizard will add member initialization here
    //}}AFX_DATA_INIT
}


void CEndpointDlg::DoDataExchange(CDataExchange* pDX)
{
    CDialog::DoDataExchange(pDX);
    //{{AFX_DATA_MAP(CEndpointDlg)
        // NOTE: the ClassWizard will add DDX and DDV calls here
    //}}AFX_DATA_MAP
}


BEGIN_MESSAGE_MAP(CEndpointDlg, CDialog)
    //{{AFX_MSG_MAP(CEndpointDlg)
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CEndpointDlg message handlers

LPCSTR CEndpointDlg::GetEndpointStr()
{
    return Endpoint;
}

void CEndpointDlg::OnOK() 
{
    GetDlgItem(IDC_SERVER_IP_EDIT)->GetWindowText(Endpoint);
    if(Endpoint.GetLength() > 0)
    {
        CString port;
        GetDlgItem(IDC_SERVER_PORT_EDIT)->GetWindowText(port);
        if(port.GetLength() > 0 && atoi(port) > 0)
        {
            Endpoint += ":";
            Endpoint += port;
            
            CDialog::OnOK();
        }
        else
            MessageBox("A port must be specified", "Endpoint Error", MB_OK | MB_ICONEXCLAMATION);
    }
    else
        MessageBox("An IP must be specified", "Endpoint Error", MB_OK | MB_ICONEXCLAMATION);
}

void CEndpointDlg::OnCancel() 
{
    Endpoint.Empty();   
    CDialog::OnCancel();
}

BOOL CEndpointDlg::OnInitDialog() 
{
    CDialog::OnInitDialog();
    GetDlgItem(IDC_SERVER_PORT_EDIT)->SetWindowText("7070");    
    return TRUE;
}
