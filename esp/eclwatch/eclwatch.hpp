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

