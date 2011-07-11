/*
 * Copyright (c) 1998,1999 Vassili Bourdo
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice in the documentation and/or other materials provided with 
 *    the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT 
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR 
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE 
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN 
 * IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef __PXSHLAPI_H_INCLUDED__
#define __PXSHLAPI_H_INCLUDED__

#ifndef _SHLOBJ_H_
#include <shlobj.h>
#endif //_SHLOBJ_H_

#ifndef _INC_SHLWAPI
#include <shlwapi.h>
#endif //_INC_SHLWAPI

#ifndef __exdisp_h__
#include <exdisp.h>
#endif //__exdisp_h__

#ifndef _INC_COMDEF
#include <comdef.h>
#endif //_INC_COMDEF

IShellFolderPtr& GetDesktopFolder();
IMallocPtr& GetMalloc();
int GetItemIDSize(LPCITEMIDLIST pidl);
LPITEMIDLIST CopyItemID(LPCITEMIDLIST pidl, int cb=-1);
LPITEMIDLIST MergeItemID(LPCITEMIDLIST pidl,...);
LPITEMIDLIST GetItemIDFromPath(LPCTSTR pszPath);
LPBYTE GetItemIDPos(LPCITEMIDLIST pidl, int nPos);
int CompareItemID(LPCITEMIDLIST pidl1,LPCITEMIDLIST pidl2);
int CompareItemID(LPCITEMIDLIST pidl1,int nSpecialFolder);
int CompareItemID(int nSpecialFolder,LPCITEMIDLIST pidl2);
HRESULT SHBindToParent(LPCITEMIDLIST pidl, REFIID riid, VOID **ppv, LPCITEMIDLIST *ppidlLast);
BOOL TrackItemIDContextMenu(LPCITEMIDLIST pidlShellItem,
    UINT nFlags, LPPOINT ptPoint, HWND hWnd);
BOOL TrackItemIDContextMenu(LPCTSTR pszShellItemPath,
    UINT nFlags, LPPOINT ptPoint, HWND hWnd);

class CPidl {
protected:
    LPITEMIDLIST m_pidl;
public:
    CPidl(): m_pidl(NULL) {}
    CPidl(LPITEMIDLIST other): m_pidl(other) {}
    CPidl(const CPidl& other): m_pidl(::CopyItemID(other.m_pidl)) {}
    CPidl(LPCTSTR pszPath): m_pidl(::GetItemIDFromPath(pszPath)) {}
    CPidl(int nSpecialFolder)
        {
            ::SHGetSpecialFolderLocation(AfxGetMainWnd()->GetSafeHwnd(),
                nSpecialFolder, &m_pidl);
        }
    virtual ~CPidl()
        { Free(); }

    LPITEMIDLIST Detach()
        { LPITEMIDLIST pidl=m_pidl; m_pidl=NULL; return pidl; }
    LPITEMIDLIST Copy()
        { return ::CopyItemID(m_pidl); }
    void Free() 
        { if( m_pidl ) { ::GetMalloc()->Free(m_pidl); m_pidl=NULL; } }
    operator bool()
        { return m_pidl != NULL; }
    operator LPITEMIDLIST()
        { return m_pidl; }
    operator LPITEMIDLIST*()
        { return &m_pidl; }
    operator LPCITEMIDLIST*()
        { return (LPCITEMIDLIST*)&m_pidl; }
    CPidl& operator=(LPITEMIDLIST other)
        { Free(); m_pidl=other; return *this; }
    CPidl& operator=(const CPidl& other)
        { Free(); m_pidl=::CopyItemID(other.m_pidl); return *this; }
    
    BOOL IsEmpty()
        { return m_pidl == NULL; }
};

#endif //__PXSHLAPI_H_INCLUDED__
