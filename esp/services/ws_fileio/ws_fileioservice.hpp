/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
        if (queryComponentConfig().getPropBool("@api_only"))
        {
            CHttpSoapBinding::getNavigationData(context, data);
            return;
        }
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

