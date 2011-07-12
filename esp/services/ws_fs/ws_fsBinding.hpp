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

// ws_fsBinding.h: interface for the CWsFsBindingEx class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_WS_FSBINDING_H__0FC58C10_AD76_4886_AB50_B76A5B4D17AA__INCLUDED_)
#define AFX_WS_FSBINDING_H__0FC58C10_AD76_4886_AB50_B76A5B4D17AA__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#pragma warning(disable:4786)
#include "ws_fs_esp.ipp"

class CFileSpraySoapBindingEx : public CFileSpraySoapBinding
{
public:
    CFileSpraySoapBindingEx(IPropertyTree* cfg, const char *bindname/*=NULL*/, const char *procname/*=NULL*/, http_soap_log_level level=hsl_none)
      : CFileSpraySoapBinding(cfg, bindname, procname, level) {}
    virtual ~CFileSpraySoapBindingEx(){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
    }

    int onGetInstantQuery(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);
    int onFinishUpload(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);

private:
    IPropertyTree* createPTreeForXslt(const char* method, const char* dfuwuid);
    static void xsltTransform(const char* xml, const char* sheet, IProperties *params, StringBuffer& ret);
    void downloadFile(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response);
};


#endif // !defined(AFX_WS_FSBINDING_H__0FC58C10_AD76_4886_AB50_B76A5B4D17AA__INCLUDED_)

