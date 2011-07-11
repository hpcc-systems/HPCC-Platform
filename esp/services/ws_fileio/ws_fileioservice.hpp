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

#ifndef _ESPWIZ_WsFileIO_HPP__
#define _ESPWIZ_WsFileIO_HPP__

#include "ws_fileio_esp.ipp"

class CWsFileIOSoapBindingEx : public CWsFileIOSoapBinding
{
public:
    CWsFileIOSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsFileIOSoapBinding(cfg, name, process, llevel){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
    }
};

class CWsFileIOEx : public CWsFileIO
{
public:
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    virtual bool onCreateFile(IEspContext &context, IEspCreateFileRequest &req, IEspCreateFileResponse &resp);
    virtual bool onWriteFileData(IEspContext &context,  IEspWriteFileDataRequest &req,  IEspWriteFileDataResponse &resp);
    virtual bool onReadFileData(IEspContext &context,     IEspReadFileDataRequest &req,   IEspReadFileDataResponse &resp);

protected:
    bool CheckServerAccess(const char* server, const char* relPath, StringBuffer& netAddr, StringBuffer& absPath);
};

#endif //_ESPWIZ_WsFileIO_HPP__

