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
    StringBuffer m_portalURL;

public:
    CFileSpraySoapBindingEx(IPropertyTree* cfg, const char *bindname/*=NULL*/, const char *procname/*=NULL*/, http_soap_log_level level=hsl_none)
      : CFileSpraySoapBinding(cfg, bindname, procname, level) {
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name='%s']/@portalurl", procname);
        const char* portalURL = cfg->queryProp(xpath.str());
        if (portalURL && *portalURL)
            m_portalURL.append(portalURL);
    }
    virtual ~CFileSpraySoapBindingEx(){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        StringBuffer path = "/WsSMC/NotInCommunityEdition?form_";
        if (m_portalURL.length() > 0)
            path.appendf("&EEPortal=%s", m_portalURL.str());

        IPropertyTree *folder0 = ensureNavFolder(data, "DFU Workunits", NULL, NULL, false, 5);
        ensureNavLink(*folder0, "Search", "/FileSpray/DFUWUSearch", "Search for DFU workunits ", NULL, NULL, 1);
        ensureNavLink(*folder0, "Browse", "/FileSpray/GetDFUWorkunits", "Browse a list of DFU workunits", NULL, NULL, 2);
        IPropertyTree *folder = ensureNavFolder(data, "DFU Files", NULL, NULL, false, 6);
        ensureNavLink(*folder, "Upload/download File", "/FileSpray/DropZoneFiles", "Upload or download File from a Drop Zone in the environment", NULL, NULL, 1);
        ensureNavLink(*folder, "View Data File", "/WsDfu/DFUGetDataColumns?ChooseFile=1", "Allows you to view the contents of a logical file", NULL, NULL, 2);
        ensureNavLink(*folder, "Search File Relationships", path.str(), "Search File Relationships", NULL, NULL, 3);
        ensureNavLink(*folder, "Browse Space Usage", "/WsDfu/DFUSpace", "View details about Space Usage", NULL, NULL, 4);
        ensureNavLink(*folder, "Search Logical Files", "/WsDfu/DFUSearch", "Search for Logical Files using a variety of search criteria", NULL, NULL, 5);
        ensureNavLink(*folder, "Browse Logical Files", "/WsDfu/DFUQuery", "Browse a list of Logical Files", NULL, NULL, 6);
        ensureNavLink(*folder, "Browse Files by Scope", "/WsDfu/DFUFileView", "Browse a list of Logical Files by Scope", NULL, NULL, 7);
        ensureNavLink(*folder, "Spray Fixed", "/FileSpray/SprayFixedInput", "Spray a fixed width file", NULL, NULL, 8);
        ensureNavLink(*folder, "Spray CSV", "/FileSpray/SprayVariableInput?submethod=csv", "Spray a comma separated value file", NULL, NULL, 9);
        ensureNavLink(*folder, "Spray XML", "/FileSpray/SprayVariableInput?submethod=xml", "Spray an XML File", NULL, NULL, 10);
        ensureNavLink(*folder, "Remote Copy", "/FileSpray/CopyInput", "Copy a Logical File from one environment to another", NULL, NULL, 11);
        ensureNavLink(*folder, "XRef", "/WsDFUXRef/DFUXRefList", "View Xref result details or run the Xref utility", NULL, NULL, 12);
    }

    int onGetInstantQuery(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);
    int onFinishUpload(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);

private:
    IPropertyTree* createPTreeForXslt(const char* method, const char* dfuwuid);
    static void xsltTransform(const char* xml, const char* sheet, IProperties *params, StringBuffer& ret);
    void downloadFile(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response);
};


#endif // !defined(AFX_WS_FSBINDING_H__0FC58C10_AD76_4886_AB50_B76A5B4D17AA__INCLUDED_)

