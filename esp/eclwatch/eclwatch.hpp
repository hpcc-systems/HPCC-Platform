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

#ifndef __ECLWATCH
#define __ECLWATCH

#include "http/platform/httpservice.hpp"
#include "workunit.hpp" 
#include "espxslt.hpp"
#include "dadfs.hpp"


class CEclWatchHttpBinding : public CEspBinding, implements EspHttpBinding
{
public:
    CEclWatchHttpBinding(IPropertyTree *cfg, const char *bindname, const char *procname);

    virtual int onPostForm(CHttpRequest* request, CHttpResponse* response);
    virtual int onGet(CHttpRequest* request, CHttpResponse* response);

private:
    struct GraphCache;
    Owned<GraphCache>  graphCache;

    StringAttr defaultUser,defaultPassword;
    Owned<IEmbeddedXslTransformer> transformer;
    Owned<IAttributeMetaDataResolver> styleCache;

    void getWorkunits(StringBuffer& xml,IProperties* params);
    void getWorkunit(const char *wuid,StringBuffer& xml,IProperties* params);
    void getWorkunitXml(const char* wuid,SCMStringBuffer& buf);
    void getWorkunitCpp(const char* wuid,MemoryBuffer& buf);
    void getWorkunitResTxt(const char* wuid,MemoryBuffer& buf);
    void getWorkunitDll(const char* wuid,MemoryBuffer& buf);
    void getWorkunitThorLog(const char *wuid,StringBuffer& log);
    void getWorkunitResults(const char* wuid,unsigned index,SCMStringBuffer& buf);
    void getWorkunitGraph(const char *wuid,const char *graphname,StringBuffer& buf);
    void getJSGraph(const char *wuid, const char *graphname, IProperties* params, StringBuffer &buf);
    void getPopups(const char *wuid, const char *graphname, StringBuffer &buf);
    void getGraphXml(const char *wuid, const char *graphname, StringBuffer &buf);
    void getDefFile(StringBuffer& file,const char* filename,IProperties* params, const char *targetType);
    void clearExceptions(const char* wuid,bool recompile);

    virtual bool isValidServiceName(IEspContext & context, const char * name){return false;}
    virtual bool qualifyServiceName(IEspContext & context, const char * servname, const char * methname, StringBuffer & servQName, StringBuffer * methQName){return false;}

     void getClusters(StringBuffer& xml,IProperties* params);
    void getThor(const char *cluster,StringBuffer& buf);
    void getThorXml(const char *cluster,StringBuffer& buf);
    void getThorLog(const char *cluster,MemoryBuffer& buf);

    void getDFU(StringBuffer& xml,IProperties* params);
    void getDFile(const char *name,StringBuffer& xml,IProperties* params);
    bool deleteFile(const char* name,StringBuffer& errs);
    bool compressFile(const char* name,StringBuffer& errs);

    bool getHTML(StringBuffer& buf,const char* xml,const char* xslname,IProperties* params);

};


#endif

