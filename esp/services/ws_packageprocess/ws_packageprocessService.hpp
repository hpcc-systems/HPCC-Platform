/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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

#ifndef _ESPWIZ_ws_packageprocess_HPP__
#define _ESPWIZ_ws_packageprocess_HPP__

#include "ws_packageprocess_esp.ipp"



class CWsPackageProcessSoapBindingEx : public CWsPackageProcessSoapBinding
{
public:
    CWsPackageProcessSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsPackageProcessSoapBinding(cfg, name, process, llevel)
    {
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        //Add navigation link here
    }
};


class CWsPackageProcessEx : public CWsPackageProcess
{
public:
    IMPLEMENT_IINTERFACE;
    virtual ~CWsPackageProcessEx(){};

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    virtual bool onEcho(IEspContext &context, IEspEchoRequest &req, IEspEchoResponse &resp);
    virtual bool onAddPackage(IEspContext &context, IEspAddPackageRequest &req, IEspAddPackageResponse &resp);
    virtual bool onDeletePackage(IEspContext &context, IEspDeletePackageRequest &req, IEspDeletePackageResponse &resp);
    virtual bool onActivatePackage(IEspContext &context, IEspActivatePackageRequest &req, IEspActivatePackageResponse &resp);
    virtual bool onDeActivatePackage(IEspContext &context, IEspDeActivatePackageRequest &req, IEspDeActivatePackageResponse &resp);
    virtual bool onListPackage(IEspContext &context, IEspListPackageRequest &req, IEspListPackageResponse &resp);
    virtual bool onGetPackage(IEspContext &context, IEspGetPackageRequest &req, IEspGetPackageResponse &resp);
    virtual bool onCopyFiles(IEspContext &context, IEspCopyFilesRequest &req, IEspCopyFilesResponse &resp);
};

#endif //_ESPWIZ_ws_packageprocess_HPP__

