#include <memory>

#include "daclient.hpp"
#include "environment.hpp"
#include "workunit.hpp"
#include "wujobq.hpp"
#include "fileview.hpp"
#include "ws_ecl_service.hpp"
#include "ws_ecl_wuinfo.hpp"
#include "xsdparser.hpp"
#include "httpclient.hpp"
#include "jsonhelpers.hpp"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5mins, 30s a bit short

#define     WSECL_ACCESS      "WsEclAccess"
static const char* WSECL_ACCESS_DENIED = "WsEcl access permission denied.";

const char *wsEclXsdTypes[] = {
    "xsd:string",
    "xsd:string",
    "xsd:boolean",
    "xsd:decimal",
    "xsd:float",
    "xsd:double",
    "xsd:duration",
    "xsd:dateTime",
    "xsd:time",
    "xsd:date",
    "xsd:gYearMonth",
    "xsd:gYear",
    "xsd:gMonthDay",
    "xsd:gDay",
    "xsd:gMonth",
    "xsd:hexBinary",
    "xsd:base64Binary",
    "xsd:anyURI",
    "xsd:QName",
    "xsd:NOTATION",
    "xsd:normalizedString",
    "xsd:token",
    "xsd:language",
    "xsd:NMTOKEN",
    "xsd:NMTOKENS",
    "xsd:Name",
    "xsd:NCName",
    "xsd:ID",
    "xsd:IDREF",
    "xsd:IDREFS",
    "xsd:ENTITY",
    "xsd:ENTITIES",
    "xsd:integer",
    "xsd:nonPositiveInteger",
    "xsd:negativeInteger",
    "xsd:long",
    "xsd:int",
    "xsd:short",
    "xsd:byte",
    "xsd:nonNegativeInteger",
    "xsd:unsignedLong",
    "xsd:unsignedInt",
    "xsd:unsignedShort",
    "xsd:unsignedByte",
    "xsd:positiveInteger",
    "tns:RawDataFile",
    "tns:CsvDataFile",
    "tns:EspStringArray",
    "tns:EspIntArray",
    "tns:XmlDataSet"
};

typedef MapStringTo<wsEclType> MapStringToWsEclType;

class wsEclTypeTranslator
{
private:
    MapStringToWsEclType typemap;
public:
    wsEclTypeTranslator();
    wsEclType translate(const char *type);
};

wsEclTypeTranslator::wsEclTypeTranslator()
{
    typemap.setValue("xsd:string",              xsdString);
    typemap.setValue("xsd:boolean",             xsdBoolean);
    typemap.setValue("xsd:decimal",             xsdDecimal);
    typemap.setValue("xsd:float",               xsdFloat);
    typemap.setValue("xsd:double",              xsdDouble);
    typemap.setValue("xsd:duration",            xsdDuration);
    typemap.setValue("xsd:dateTime",            xsdDateTime);
    typemap.setValue("xsd:time",                xsdTime);
    typemap.setValue("xsd:date",                xsdDate);
    typemap.setValue("xsd:gyearmonth",          xsdYearMonth);
    typemap.setValue("xsd:gyear",               xsdYear);
    typemap.setValue("xsd:gmonthday",           xsdMonthDay);
    typemap.setValue("xsd:gday",                xsdDay);
    typemap.setValue("xsd:gmonth",              xsdMonth);
    typemap.setValue("xsd:hexbinary",           xsdHexBinary);
    typemap.setValue("xsd:base64binary",        xsdBase64Binary);
    typemap.setValue("xsd:anyuri",              xsdAnyURI);
    typemap.setValue("xsd:qname",               xsdQName);
    typemap.setValue("xsd:notation",            xsdNOTATION);
    typemap.setValue("xsd:normalizedstring",    xsdNormalizedString);
    typemap.setValue("xsd:token",               xsdToken);
    typemap.setValue("xsd:language",            xsdLanguage);
    typemap.setValue("xsd:nmtoken",             xsdNMTOKEN);
    typemap.setValue("xsd:nmtokens",            xsdNMTOKENS);
    typemap.setValue("xsd:name",                xsdName);
    typemap.setValue("xsd:ncname",              xsdNCName);
    typemap.setValue("xsd:id",                  xsdID);
    typemap.setValue("xsd:idref",               xsdIDREF);
    typemap.setValue("xsd:idrefs",              xsdIDREFS);
    typemap.setValue("xsd:entity",              xsdENTITY);
    typemap.setValue("xsd:entities",            xsdENTITIES);
    typemap.setValue("xsd:integer",             xsdInteger);
    typemap.setValue("xsd:nonpositiveinteger",  xsdNonPositiveInteger);
    typemap.setValue("xsd:negativeinteger",     xsdNegativeInteger);
    typemap.setValue("xsd:long",                xsdLong);
    typemap.setValue("xsd:int",                 xsdInt);
    typemap.setValue("xsd:short",               xsdShort);
    typemap.setValue("xsd:byte",                xsdByte);
    typemap.setValue("xsd:nonnegativeinteger",  xsdNonNegativeInteger);
    typemap.setValue("xsd:unsignedlong",        xsdUnsignedLong);
    typemap.setValue("xsd:unsignedint",         xsdUnsignedInt);
    typemap.setValue("xsd:unsignedshort",       xsdUnsignedShort);
    typemap.setValue("xsd:unsignedbyte",        xsdUnsignedByte);
    typemap.setValue("xsd:positiveinteger",     xsdPositiveInteger);

    typemap.setValue("tns:rawdatafile",         tnsRawDataFile);
    typemap.setValue("tns:csvdatafile",         tnsCsvDataFile);
    typemap.setValue("tns:espstringarray",      tnsEspStringArray);
    typemap.setValue("tns:espintarray",         tnsEspIntArray);
    typemap.setValue("tns:xmldataset",          tnsXmlDataSet);
}

wsEclType wsEclTypeTranslator::translate(const char *type)
{
    if (!type || !*type)
        return wsEclTypeUnknown;

    StringBuffer value(type);
    wsEclType *ret = typemap.getValue(value.toLowerCase().str());

    return (ret) ? *ret : wsEclTypeUnknown;
}


const char *wsEclToXsdTypes(wsEclType from);

static wsEclTypeTranslator *translator = NULL;

const char *wsEclToXsdTypes(wsEclType from)
{
    if (from < maxWsEclType)
        return wsEclXsdTypes[from];
    return wsEclXsdTypes[wsEclTypeUnknown];
}

const char *translateXsdType(const char *from)
{
    return wsEclToXsdTypes(translator->translate(from));
}


// Interestingly, only single quote needs to HTML escape.
// ", <, >, & don't need escape.
static void escapeSingleQuote(StringBuffer& src, StringBuffer& escaped)
{
    for (const char* p = src.str(); *p!=0; p++)
    {
        if (*p == '\'')
            escaped.append("&apos;");
        else
            escaped.append(*p);
    }
}

static void appendServerAddress(StringBuffer &s, IPropertyTree &env, IPropertyTree &server, const char *daliAddress)
{
    const char *port = server.queryProp("@port");
    if (port && streq(port, "0")) //roxie on demand
        return;
    const char *netAddress = server.queryProp("@netAddress");
    if (!netAddress && server.hasProp("@computer"))
    {
        VStringBuffer xpath("Hardware/Computer[@name='%s']/@netAddress", server.queryProp("@computer"));
        netAddress = env.queryProp(xpath.str());
    }
    if ((!netAddress || *netAddress=='.') && daliAddress && *daliAddress)
        netAddress = daliAddress;
    if (!netAddress || !*netAddress)
        return;
    if (s.length())
        s.append('|');
    s.append(netAddress).append(':').append(port ? port : "9876");
}

bool CWsEclService::init(const char * name, const char * type, IPropertyTree * cfg, const char * process)
{
    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name='%s']", process);
    IPropertyTree *prc = cfg->queryPropTree(xpath.str());
    if (!prc)
        throw MakeStringException(-1, "ESP Process %s not configured", process);

    auth_method.set(prc->queryProp("Authentication/@method"));
    portal_URL.set(prc->queryProp("@portalurl"));

    StringBuffer daliAddress;
    const char *daliServers = prc->queryProp("@daliServers");
    if (daliServers)
    {
        while (*daliServers && !strchr(":;,", *daliServers))
            daliAddress.append(*daliServers++);
    }

    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> environment = factory->openEnvironment();
    Owned<IPropertyTree> pRoot = &environment->getPTree();

    xpath.clear().appendf("EspService[@name='%s']", name);
    IPropertyTree *serviceTree = prc->queryPropTree(xpath);
    if (!serviceTree)
        throw MakeStringException(-1, "ESP Service %s not configured", name);

    roxieTimeout = serviceTree->getPropInt("RoxieTimeout", 10 * 60);
    if (!roxieTimeout)
        roxieTimeout = WAIT_FOREVER;
    workunitTimeout = serviceTree->getPropInt("WorkunitTimeout", 10 * 60);
    if (workunitTimeout)
        workunitTimeout *= 1000;
    else
        workunitTimeout = WAIT_FOREVER;

    const char *headerName = serviceTree->queryProp("HttpGlobalIdHeader");
    if (headerName && *headerName && !streq(headerName, "HPCC-Global-Id")) //default will be checked anyway
        globalIdHttpHeader.set(headerName);
    headerName = serviceTree->queryProp("HttpCallerIdHeader");
    if (headerName && *headerName && !streq(headerName, "HPCC-Caller-Id")) //default will be checked anyway
        callerIdHttpHeader.set(headerName);

    Owned<IPropertyTreeIterator> cfgTargets = serviceTree->getElements("Targets/Target");
    ForEach(*cfgTargets)
        targets.append(cfgTargets->query().queryProp(NULL));

    IPropertyTree *vips = serviceTree->queryPropTree("VIPS");
    Owned<IStringIterator> roxieTargets = getTargetClusters("RoxieCluster", NULL);

    ForEach(*roxieTargets)
    {
        SCMStringBuffer target;
        roxieTargets->str(target);
        if (!target.length() || connMap.getValue(target.str())) //bad config?
            continue;
        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(target.str());
        if (!clusterInfo)
            continue;
        SCMStringBuffer process;
        clusterInfo->getRoxieProcess(process);
        if (!process.length())
            continue;
        const char *vip = NULL;
        bool includeTargetInURL = true;
        unsigned dnsInterval = (unsigned) -1;
        if (vips)
        {
            IPropertyTree *pc = vips->queryPropTree(xpath.clear().appendf("ProcessCluster[@name='%s']", process.str()));
            if (pc)
            {
                vip = pc->queryProp("@vip");
                includeTargetInURL = pc->getPropBool("@includeTargetInURL", true);
                dnsInterval = (unsigned) pc->getPropInt("@dnsInterval", -1);

            }
        }
        StringBuffer list;
        bool loadBalanced = false;
        if (vip && *vip)
        {
            list.append(vip);
            loadBalanced = true;
        }
        else
        {
            VStringBuffer xpath("Software/RoxieCluster[@name='%s']", process.str());
            Owned<IPropertyTreeIterator> it = pRoot->getElements(xpath.str());
            ForEach(*it)
            {
                Owned<IPropertyTreeIterator> servers = it->query().getElements("RoxieServerProcess");
                ForEach(*servers)
                    appendServerAddress(list, *pRoot, servers->query(), daliAddress.str());
            }
        }
        if (list.length())
        {
            StringAttr alias(clusterInfo->getAlias());
            Owned<ISmartSocketFactory> sf = new RoxieSocketFactory(list.str(), !loadBalanced, includeTargetInURL, loadBalanced ? alias.str() : NULL, dnsInterval);
            connMap.setValue(target.str(), sf.get());
            if (alias.length() && !connMap.getValue(alias.str())) //only need one vip per alias for routing purposes
                connMap.setValue(alias.str(), sf.get());
        }
    }

    translator = new wsEclTypeTranslator();

    Owned<IPropertyTreeIterator> xsltProps = serviceTree->getElements("xslt*");
    ForEach(*xsltProps)
        xsltConfig->addPropTree(xsltProps->query().queryName(), LINK(&xsltProps->query()));

    return true;
}

CWsEclService::~CWsEclService()
{
    if (translator)
        delete translator;
}


void CWsEclBinding::getNavigationData(IEspContext &context, IPropertyTree & data)
{
    DBGLOG("CScrubbedXmlBinding::getNavigationData");

    StringArray wsModules;
    StringBuffer mode;

    data.addProp("@viewType", "wsecl_tree");
    data.addProp("@action", "NavMenuEvent");
    data.addProp("@appName", "WsECL 3.0");

    ensureNavDynFolder(data, "Targets", "Targets", "root=true", NULL);
}

void CWsEclBinding::getRootNavigationFolders(IEspContext &context, IPropertyTree & data)
{
    DBGLOG("CScrubbedXmlBinding::getNavigationData");

    StringArray wsModules;
    StringBuffer mode;

    data.addProp("@viewType", "wsecl_tree");
    data.addProp("@action", "NavMenuEvent");
    data.addProp("@appName", "WsECL 3.0");

    Owned<IStringIterator> envTargets = getTargetClusters(NULL, NULL);

    SCMStringBuffer target;
    ForEach(*envTargets)
    {
        envTargets->str(target);
        if (wsecl->targets.length() && !wsecl->targets.contains(target.str()))
            continue;
        VStringBuffer parms("queryset=%s", target.str());
        ensureNavDynFolder(data, target.str(), target.str(), parms.str(), NULL);
    }
}

void CWsEclBinding::addQueryNavLink(IPropertyTree &data, IPropertyTree *query, const char *setname, const char *qname)
{
    if (!query)
        return;
    if (query->getPropBool("@suspended"))
        return;
    if (!qname || !*qname)
        qname = query->queryProp("@id");
    if (!setname || !*setname || !qname || !*qname)
        return;

    StringBuffer navPath;
    navPath.appendf("/WsEcl/tabview/query/%s/%s", setname, qname);
    ensureNavLink(data, qname, navPath.str(), qname, "menu2", navPath.str());
}

void CWsEclBinding::getQueryNames(IPropertyTree* settree, const char *id, const char *qname, StringArray& qnames)
{
    if (!id || !*id)
        return;

    VStringBuffer xpath("Query[@id='%s']", id);
    IPropertyTree *query = settree->queryPropTree(xpath.str());
    if (!query || query->getPropBool("@isLibrary") || query->getPropBool("@suspended"))
        return;

    if (!qname || !*qname)
        qname = query->queryProp("@id");

    if (!qname || !*qname)
        return;

    qnames.append(qname);
}

void CWsEclBinding::getDynNavData(IEspContext &context, IProperties *params, IPropertyTree & data)
{
    if (!params)
        return;

    data.setPropBool("@volatile", true);
    if (params->getPropBool("root", false))
    {
        getRootNavigationFolders(context, data);
    }
    else if (params->hasProp("queryset"))
    {
        const char *setname = params->queryProp("queryset");
        if (!setname || !*setname)
            return;
        Owned<IPropertyTree> settree = getQueryRegistry(setname, true);
        if (!settree)
            return;

        if (params->hasProp("QueryList"))
        {
            Owned<IPropertyTreeIterator> iter = settree->getElements("Query");
            ForEach(*iter)
            {
                if (!iter->query().getPropBool("@isLibrary"))
                    addQueryNavLink(data, &iter->query(), setname);
            }
        }
        else
        {
            StringArray qnames;
            Owned<IPropertyTreeIterator> iter = settree->getElements("Alias");
            ForEach(*iter)
            {
                IPropertyTree &alias = iter->query();
                getQueryNames(settree, alias.queryProp("@id"), alias.queryProp("@name"), qnames);
            }
            if (qnames.ordinality())
            {
                qnames.sortAscii();
                ForEachItemIn(i,qnames)
                {
                    StringBuffer navPath;
                    const char *qname = qnames.item(i);
                    navPath.appendf("/WsEcl/tabview/query/%s/%s", setname, qname);
                    ensureNavLink(data, qname, navPath.str(), qname, "menu2", navPath.str());
                }
            }
        }
    }
}

static void splitPathTailAndExt(const char *s, StringBuffer &path, StringBuffer &tail, StringBuffer *ext)
{
    if (s)
    {
        const char *finger=s;
        while (*finger++);
        const char *extpos=finger;
        const char *tailpos=s;
        while (s!=finger && tailpos==s)
        {
            switch (*finger)
            {
                case '.':
                    if (ext && *extpos!='.')
                        extpos=finger;
                    break;
                case '/':
                case '\\':
                    tailpos=finger;
                    break;
            }
            finger--;
        }
        if (ext && *extpos=='.')
            ext->append(extpos+1);
        if (tailpos!=s)
            path.append(tailpos - s, s);
        if (strchr("\\/", *tailpos))
            tailpos++;
        tail.append(extpos - tailpos, tailpos);
    }
}

static void splitLookupInfo(IProperties *parms, const char *&s, StringBuffer &wuid, StringBuffer &qs, StringBuffer &qid)
{
    StringBuffer lookup;
    nextPathNode(s, lookup);

    if (strieq(lookup.str(), "query"))
    {
        nextPathNode(s, qs);
        nextPathNode(s, qid);
    }
    else if (strieq(lookup.str(), "wuid"))
    {
        nextPathNode(s, wuid);
        qs.append(parms->queryProp("qset"));
        qid.append(parms->queryProp("qname"));
    }
}

void CWsEclBinding::xsltTransform(const char* xml, unsigned int len, const char* xslFileName, IProperties *params, StringBuffer& ret)
{
    Owned<IXslProcessor> proc  = getXslProcessor();
    Owned<IXslTransform> trans = proc->createXslTransform(queryXsltConfig());

    trans->setXmlSource(xml, len);

    StringBuffer xslpath(getCFD());
    xslpath.append(xslFileName);
    trans->loadXslFromFile(xslpath.str());

    if (params)
    {
        Owned<IPropertyIterator> it = params->getIterator();
        for (it->first(); it->isValid(); it->next())
        {
            const char *key = it->getPropKey();
            //set parameter in the XSL transform skipping over the @ prefix, if any
            const char* paramName = *key == '@' ? key+1 : key;
            trans->setParameter(paramName, StringBuffer().append('\'').append(params->queryProp(key)).append('\'').str());
        }
    }

    trans->transform(ret);
}

StringBuffer &CWsEclBinding::generateNamespace(IEspContext &context, CHttpRequest* request, const char *serv, const char *method, StringBuffer &ns)
{
    ns.append("urn:hpccsystems:ecl:");
    if (method && *method)
        ns.appendLower(strlen(method), method);
    return ns;
}


#define REQSF_ROOT         0x0001
#define REQSF_SAMPLE_DATA  0x0002
#define REQSF_TRIM         0x0004
#define REQSF_ESCAPEFORMATTERS 0x0008
#define REQSF_EXCLUSIVE (REQSF_SAMPLE_DATA | REQSF_TRIM)

static void buildReqXml(StringArray& parentTypes, IXmlType* type, StringBuffer& out, const char* tag, IPropertyTree *reqTree, unsigned flags, const char* ns=NULL)
{
    assertex(type!=NULL);
    assertex((flags & REQSF_EXCLUSIVE)!= REQSF_EXCLUSIVE);

    if (!reqTree && (flags & REQSF_TRIM) && !(flags & REQSF_ROOT))
        return;

    const char* typeName = type->queryName();
    if (type->isComplexType())
    {
        if (typeName && !parentTypes.appendUniq(typeName))
            return; // recursive

        int startlen = out.length();
        appendXMLOpenTag(out, tag, NULL, false);
        if (ns)
            out.append(' ').append(ns);
        int taglen=out.length()+1;
        for (size_t i=0; i<type->getAttrCount(); i++)
        {
            IXmlAttribute* attr = type->queryAttr(i);
            StringBuffer s;
            const char *attrval;
            if (reqTree)
                attrval = reqTree->queryProp(s.append('@').append(attr->queryName()));
            else
                attrval = attr->getSampleValue(s);
            if (attrval)
                appendXMLAttr(out, attr->queryName(), attrval, nullptr, true);
        }
        if (flags & REQSF_ROOT)
        {
            bool log = reqTree->getPropBool("@log", false);
            if (log)
                appendXMLAttr(out, "log", "true", nullptr, true);
            int tracelevel = reqTree->getPropInt("@traceLevel", -1);
            if (tracelevel >= 0)
                out.appendf(" traceLevel=\"%d\"", tracelevel);
        }
        out.append('>');

        int flds = type->getFieldCount();
        switch (type->getSubType())
        {
        case SubType_Complex_SimpleContent:
            assertex(flds==0);
            if (reqTree)
            {
                const char *val = reqTree->queryProp(NULL);
                if (val)
                    encodeXML(val, out);
            }
            else if (flags & REQSF_SAMPLE_DATA)
                type->queryFieldType(0)->getSampleValue(out,tag);
            break;

        default:
            for (int idx=0; idx<flds; idx++)
            {
                IPropertyTree *childtree = NULL;
                const char *childname = type->queryFieldName(idx);
                if (reqTree)
                    childtree = reqTree->queryPropTree(childname);
                buildReqXml(parentTypes,type->queryFieldType(idx), out, childname, childtree, flags & ~REQSF_ROOT);
            }
            break;
        }

        if (typeName)
            parentTypes.pop();
        if ((flags & REQSF_TRIM) && !(flags & REQSF_ROOT) && out.length()==taglen)
            out.setLength(startlen);
        else
            appendXMLCloseTag(out, tag);
    }
    else if (type->isArray())
    {
        if (typeName && !parentTypes.appendUniq(typeName))
            return; // recursive

        const char* itemName = type->queryFieldName(0);
        IXmlType*   itemType = type->queryFieldType(0);
        if (!itemName || !itemType)
            throw MakeStringException(-1,"*** Invalid array definition: tag=%s, itemName=%s", tag, itemName?itemName:"NULL");

        int startlen = out.length();
        appendXMLOpenTag(out, tag, NULL, false);
        if (ns)
            out.append(' ').append(ns);
        out.append(">");
        int taglen=out.length();
        if (reqTree)
        {
            Owned<IPropertyTreeIterator> items = reqTree->getElements(itemName);
            ForEach(*items)
                buildReqXml(parentTypes,itemType,out,itemName, &items->query(), flags & ~REQSF_ROOT);
        }
        else
            buildReqXml(parentTypes,itemType,out,itemName, NULL, flags & ~REQSF_ROOT);

        if (typeName)
            parentTypes.pop();
        if ((flags & REQSF_TRIM) && !(flags & REQSF_ROOT) && out.length()==taglen)
            out.setLength(startlen);
        else
            appendXMLCloseTag(out, tag);
    }
    else // simple type
    {
        StringBuffer parmval;
        if (reqTree)
            parmval.append(reqTree->queryProp(NULL));
        if (!parmval.length() && (flags & REQSF_SAMPLE_DATA))
            type->getSampleValue(parmval, NULL);
        
        if (parmval.length() || !(flags&REQSF_TRIM))
        {
            if (strieq(typeName, "boolean"))
            {
                if (!strieq(parmval, "default"))
                    appendXMLTag(out, tag, strToBool(parmval.str()) ? "1" : "0");
            }
            else
                appendXMLTag(out, tag, parmval);
        }
    }
}

void appendRESTParameter(StringBuffer &out, const StringArray &path, const char *name, const char *value)
{
    StringBuffer s;
    ForEachItemIn(i, path)
    {
        if (s.length())
            s.append('.');
        s.append(path.item(i));
    }
    if (name && *name)
    {
        if (s.length())
            s.append('.');
        s.append(name);
    }
    if (!s.length())
        return;
    unsigned len = out.length();
    if (len && out.charAt(len-1)!='?')
        out.append('&');
    out.append(s).append('=');
    if (value && *value)
        appendURL(&out, value);
}

static void buildRestURL(StringArray& parentTypes, StringArray &path, IXmlType* type, StringBuffer& out, const char* tag, unsigned flags, unsigned depth=0)
{
    assertex(type!=NULL);

    const char* typeName = type->queryName();
    if (type->isComplexType())
    {
        if (typeName && !parentTypes.appendUniq(typeName))
            return; // recursive
        if (tag && *tag)
            path.append(tag);
        for (size_t i=0; i<type->getAttrCount(); i++)
        {
            IXmlAttribute* attr = type->queryAttr(i);
            StringBuffer s;
            const char *attrval=NULL;
            if (flags & REQSF_SAMPLE_DATA)
                attrval = attr->getSampleValue(s);
            appendRESTParameter(out, path, attr->queryName(), attrval);
        }

        int flds = type->getFieldCount();
        switch (type->getSubType())
        {
        case SubType_Complex_SimpleContent:
            assertex(flds==0);
            {
                StringBuffer val;
                if (flags & REQSF_SAMPLE_DATA)
                    type->queryFieldType(0)->getSampleValue(val, tag);
                appendRESTParameter(out, path, tag, val);
            }
            break;

        default:
            for (int idx=0; idx<flds; idx++)
                buildRestURL(parentTypes, path, type->queryFieldType(idx), out, type->queryFieldName(idx), flags, depth+1);
            break;
        }

        if (typeName)
            parentTypes.pop();
        if (tag && *tag)
            path.pop();
    }
    else if (type->isArray())
    {
        if (typeName && !parentTypes.appendUniq(typeName))
            return; // recursive
        if (tag && *tag)
            path.append(tag);

        const char* itemName = type->queryFieldName(0);
        IXmlType*   itemType = type->queryFieldType(0);
        if (!itemName || !itemType)
            throw MakeStringException(-1,"*** Invalid array definition: tag=%s, itemName=%s", tag, itemName?itemName:"NULL");

        StringBuffer itemURLPath(itemName);
        if (depth>1)
            itemURLPath.append(".0");
        buildRestURL(parentTypes, path, itemType, out, itemURLPath, flags, depth+1);

        if (typeName)
            parentTypes.pop();
        if (tag && *tag)
            path.pop();
    }
    else // simple type
    {
        StringBuffer parmval;
        if (flags & REQSF_SAMPLE_DATA)
            type->getSampleValue(parmval, NULL);

        if (strieq(typeName, "boolean"))
        {
            if (!strieq(parmval, "default"))
                appendRESTParameter(out, path, tag, strToBool(parmval.str()) ? "1" : "0");
        }
        else
            appendRESTParameter(out, path, tag, parmval);
    }
}

static inline StringBuffer &appendNamespaceSpecificString(StringBuffer &dest, const char *src)
{
    if (src)
        while(*src){
            dest.append((const char)(isspace(*src) ? '_' : tolower(*src)));
            src++;
        }
    return dest;
}

void buildSampleDataset(StringBuffer &xml, IPropertyTree *xsdtree, const char *service, const char *method, const char *resultname)
{
    StringBuffer schemaXml;
    toXML(xsdtree, schemaXml);

    Owned<IXmlSchema> schema = createXmlSchemaFromString(schemaXml);
    if (schema.get())
    {
        IXmlType* type = schema->queryElementType("Dataset");
        if (type)
        {
            StringBuffer ns("xmlns=\"urn:hpccsystems:ecl:");
            appendNamespaceSpecificString(ns, method).append(":result:");
            appendNamespaceSpecificString(ns, resultname);
            ns.append('\"');
            StringArray parentTypes;
            buildReqXml(parentTypes, type, xml, "Dataset", NULL, REQSF_SAMPLE_DATA, ns.str());
        }
    }

}

void buildSampleJsonDataset(StringBuffer &json, IPropertyTree *xsdtree, const char *service, const char *method, const char *resultname)
{
    StringBuffer schemaXml;
    toXML(xsdtree, schemaXml);

    Owned<IXmlSchema> schema = createXmlSchemaFromString(schemaXml);
    if (schema.get())
    {
        IXmlType* type = schema->queryElementType("Dataset");
        if (type)
        {
            StringArray parentTypes;
            delimitJSON(json, true);
            JsonHelpers::buildJsonMsg(parentTypes, type, json, resultname, NULL, REQSF_SAMPLE_DATA);
        }
    }

}

void CWsEclBinding::buildSampleResponseXml(StringBuffer& msg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo)
{
    StringBuffer element;
    element.append(wsinfo.queryname.str()).append("Response");

    StringBuffer xsds;
    wsinfo.getSchemas(xsds);

    msg.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    if (context.queryRequestParameters()->hasProp("display"))
        msg.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");

    msg.append('<').append(element.str()).append(" xmlns=\"urn:hpccsystems:ecl:");
    msg.appendLower(wsinfo.queryname.length(), wsinfo.queryname.str()).append("\">");
    msg.append("<Results><Result>");

    Owned<IPropertyTree> xsds_tree;
    if (xsds.length())
        xsds_tree.setown(createPTreeFromXMLString(xsds.str()));

    if (xsds_tree)
    {
        Owned<IPropertyTreeIterator> result_xsds =xsds_tree->getElements("Result");
        ForEach (*result_xsds)
            buildSampleDataset(msg, result_xsds->query().queryPropTree("xs:schema"), wsinfo.qsetname.str(), wsinfo.queryname.str(), result_xsds->query().queryProp("@name"));
    }

    msg.append("</Result></Results>");
    msg.append("</").append(element.str()).append('>');
}


int CWsEclBinding::getWsEclLinks(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo)
{
    StringBuffer xml;
    xml.append("<links>");
    xml.append("<version>3</version>");
    xml.append("<path>").append(wsinfo.qsetname.str()).append("</path>");
    xml.append("<query>").append(wsinfo.queryname.str()).append("</query>");

    StringBuffer xsds;
    wsinfo.getSchemas(xsds);

    Owned<IPropertyTree> xsdtree;
    if (xsds.length())
        xsdtree.setown(createPTreeFromXMLString(xsds.str()));

    if (xsdtree)
    {
        xml.append("<input_datasets>");
        Owned<IPropertyTreeIterator> input_xsds =xsdtree->getElements("Input");
        ForEach (*input_xsds)
        {
            xml.append("<dataset>");
            xml.append("<name>").append(input_xsds->query().queryProp("@sname")).append("</name>");
            xml.append("</dataset>");
        }
        xml.append("</input_datasets>");
        xml.append("<result_datasets>");
        Owned<IPropertyTreeIterator> result_xsds =xsdtree->getElements("Result");
        ForEach (*result_xsds)
        {
            xml.append("<dataset>");
            xml.append("<name>").append(result_xsds->query().queryProp("@sname")).append("</name>");
            xml.append("</dataset>");
        }
        xml.append("</result_datasets>");
    }

    xml.append("</links>");

    Owned<IXslProcessor> xslp = getXslProcessor();
    Owned<IXslTransform> xform = xslp->createXslTransform(queryXsltConfig());
    xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/wsecl3_links.xslt").str());
    xform->setXmlSource(xml.str(), xml.length());

    StringBuffer page;
    xform->transform(page);

    response->setContent(page);
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);
    response->send();

    return 0;
}


int CWsEclBinding::getWsEcl2TabView(CHttpRequest* request, CHttpResponse* response, const char *thepath)
{
    IEspContext *context = request->queryContext();
    IProperties *parms = request->queryParameters();

    StringBuffer wuid;
    StringBuffer qs;
    StringBuffer qid;
    splitLookupInfo(request->queryParameters(), thepath, wuid, qs, qid);

    WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());
    const char *w = wsinfo.ensureWuid();

    StringBuffer xml;
    xml.append("<tabview>");
    xml.append("<version>3</version>");
    xml.appendf("<wuid>%s</wuid>", w);
    xml.appendf("<qset>%s</qset>", wsinfo.qsetname.str());
    xml.appendf("<qname>%s</qname>", wsinfo.queryname.str());

    StringBuffer xsds;
    wsinfo.getSchemas(xsds);
    Owned<IPropertyTree> xsdtree;
    if (xsds.length())
        xsdtree.setown(createPTreeFromXMLString(xsds.str()));

    if (xsdtree)
    {
        xml.append("<input_datasets>");
        Owned<IPropertyTreeIterator> input_xsds =xsdtree->getElements("Input");
        ForEach (*input_xsds)
        {
            xml.append("<dataset>");
            xml.append("<name>").append(input_xsds->query().queryProp("@name")).append("</name>");
            xml.append("</dataset>");
        }
        xml.append("</input_datasets>");
        xml.append("<result_datasets>");
        Owned<IPropertyTreeIterator> result_xsds =xsdtree->getElements("Result");
        ForEach (*result_xsds)
        {
            xml.append("<dataset>");
            xml.append("<name>").append(result_xsds->query().queryProp("@name")).append("</name>");
            xml.append("</dataset>");
        }
        xml.append("</result_datasets>");
    }
    xml.append("</tabview>");

    StringBuffer html;
    xsltTransform(xml.str(), xml.length(), "./xslt/wsecl3_tabview.xsl", NULL, html);
    response->setStatus("200 OK");
    response->setContent(html.str());
    response->setContentType("text/html");
    response->send();

    return 0;
}

void CWsEclBinding::appendSchemaNamespaces(IPropertyTree *namespaces, IEspContext &ctx, CHttpRequest* req, const char *service, const char *method)
{
    WsEclWuInfo *wsinfo = (WsEclWuInfo *) ctx.getBindingValue();
    if (wsinfo)
        appendSchemaNamespaces(namespaces, ctx, req, *wsinfo);
}


void CWsEclBinding::appendSchemaNamespaces(IPropertyTree *namespaces, IEspContext &ctx, CHttpRequest* req, WsEclWuInfo &wsinfo)
{
    IProperties *parms = ctx.queryRequestParameters();
        StringBuffer xsds;
        wsinfo.getSchemas(xsds);

        Owned<IPropertyTree> xsdtree;
        if (xsds.length())
            xsdtree.setown(createPTreeFromXMLString(xsds.str()));

        if (xsdtree)
        {
            Owned<IPropertyTreeIterator> result_xsds =xsdtree->getElements("Result");
            int count=1;
            ForEach (*result_xsds)
            {
                const char *resultname = result_xsds->query().queryProp("@sname");
                StringBuffer urn("urn:hpccsystems:ecl:");
                appendNamespaceSpecificString(urn, wsinfo.queryname.get()).append(":result:");
                appendNamespaceSpecificString(urn, resultname);
                VStringBuffer nsxml("<namespace nsvar=\"ds%d\" ns=\"%s\" import=\"1\" location=\"../result/%s.xsd\"/>", count++, urn.toLowerCase().str(), resultname);
                namespaces->addPropTree("namespace", createPTreeFromXMLString(nsxml.str()));
            }
        }
}

StringBuffer &appendEclXsdName(StringBuffer &content, const char *name, bool istype=false)
{
    if (name)
    {
        if (!strnicmp(name, "xs:", 3))
            content.append("xsd:").append(name+3);
        else
        {
            if (istype && !strchr(name, ':'))
                content.append("tns:");
            content.append(name);
        }
    }
    return content;
}

void appendEclXsdStartTag(StringBuffer &content, IPropertyTree *element, int indent, const char *attrstr=NULL, bool forceclose=false)
{
    const char *name = element->queryName();
    appendEclXsdName(content.append('<'), name);
    if (strieq(name, "xs:element"))
    {
        const char *elname=element->queryProp("@name");
        if (!elname || !*elname) //ecl bug?
            element->setProp("@name", "__unknown");
        if (!element->hasProp("@minOccurs"))
            content.append(' ').append("minOccurs=\"0\"");
    }
    if (attrstr)
        content.append(' ').append(attrstr);
    Owned<IAttributeIterator> attrs = element->getAttributes();
    ForEach(*attrs)
    {
        const char *attrname=attrs->queryName()+1;
        appendEclXsdName(content.append(' '), attrname);
        appendEclXsdName(content.append("=\""), attrs->queryValue(), !stricmp(attrname, "type")).append('\"');
    }
    if (forceclose || !element->hasChildren())
        content.append('/');
    content.append(">");
}



void appendEclXsdComplexType(StringArray &names, StringBuffer &content, IPropertyTree *element, int indent=0)
{
    StringBuffer name("t_");
    ForEachItemIn(idx, names)
    {
        name.append(names.item(idx));
    }
    content.appendf("<xsd:complexType name=\"%s\">", name.str());

    Owned<IPropertyTreeIterator> children = element->getElements("*");
    ForEach(*children)
    {
        IPropertyTree &child = children->query();
        appendEclXsdStartTag(content, &child, indent);
        if (strieq(child.queryName(), "xs:sequence") || strieq(child.queryName(), "xs:all"))
        {
            Owned<IPropertyTreeIterator> els = child.getElements("xs:element");
            ForEach(*els)
            {
                IPropertyTree &el = els->query();
                StringBuffer typeattr;
                if (!el.hasProp("@type") && el.hasProp("xs:complexType") && el.hasProp("@name"))
                    typeattr.appendf("type=\"tns:%s%s\"", name.str(), el.queryProp("@name"));
                appendEclXsdStartTag(content, &el, indent, typeattr.str(), true);
            }
        }
        if (child.hasChildren())
        {
            content.appendf("</");
            appendEclXsdName(content, child.queryName());
            content.append(">");
        }
    }

    content.append("</xsd:complexType>");
}



void appendEclXsdNestedComplexTypes(StringArray &names, StringBuffer &content, IPropertyTree *element, int indent=0)
{
    if (element->hasChildren())
    {
        Owned<IPropertyTreeIterator> children = element->getElements("*");
        ForEach(*children)
        {
            if (element->hasProp("@name"))
                names.append(element->queryProp("@name"));
            appendEclXsdNestedComplexTypes(names, content, &children->query(), indent+1);
            if (element->hasProp("@name"))
                names.pop();
        }
        if (strieq(element->queryName(), "xs:complexType"))
            appendEclXsdComplexType(names, content, element, indent);
    }
}

void appendEclXsdSection(StringBuffer &content, IPropertyTree *element, int indent=0)
{
    appendEclXsdStartTag(content, element, indent);
    if (element->hasChildren())
    {
        Owned<IPropertyTreeIterator> children = element->getElements("*");
        ForEach(*children)
        {
            appendEclXsdSection(content, &children->query(), indent+1);
        }
        appendEclXsdName(content.append("</"), element->queryName()).append('>');
    }
}

void appendEclInputXsds(StringBuffer &content, IPropertyTree *xsd, BoolHash &added)
{
    Owned<IPropertyTreeIterator> it = xsd->getElements("xs:schema/*");
    const char *schema_name=xsd->queryProp("@name");
    if (schema_name && *schema_name)
    {
        ForEach (*it)
        {
            IPropertyTree &item = it->query();
            StringArray names;
            names.append(schema_name);
            appendEclXsdNestedComplexTypes(names, content, &item, 1);

            const char *aname = item.queryProp("@name");
            StringBuffer temp;
            const char *elname = item.getName(temp).str();
            if (!stricmp(aname, "dataset") || !added.getValue(aname))
            {
                StringBuffer temp;
                if (!stricmp(aname, "dataset"))
                {
#if 0
                    content.appendf("<xsd:complexType name=\"%s\"><xsd:sequence>", schema_name);
                    Owned<IPropertyTreeIterator> children = item.getElements("xs:complexType/xs:sequence/*");
                    ForEach(*children)
                    {
                        IPropertyTree &child = children->query();
                        if (child.hasProp("@name") && !stricmp(child.queryProp("@name"), "Row"))
                        {
                            child.setProp("@minOccurs", "0");
                            child.setProp("@maxOccurs", "unbounded");
                        }
                        //appendEclXsdSection(content, &child, 2);
                        //toXML(&child, content);
                    }
                    content.appendf("</xsd:sequence></xsd:complexType>", aname);
#endif
                }
                else
                {
                    added.setValue(aname, true);
                    appendEclXsdSection(content, &item, 1);
                    //toXML(&item, content);
                }
            }
        }
    }
}

void CWsEclBinding::SOAPSectionToXsd(WsEclWuInfo &wuinfo, IPropertyTree *parmTree, StringBuffer &schema, bool isRequest, IPropertyTree *xsdtree)
{
    schema.appendf("<xsd:element name=\"%s%s\">", wuinfo.queryname.str(), isRequest ? "Request" : "Response");
    schema.append("<xsd:complexType>");
    schema.append("<xsd:all>");
    Owned<IPropertyTreeIterator> parts = parmTree->getElements("part");
    if (parts)
    {
        ForEach(*parts)
        {
            IPropertyTree &part = parts->query();
            const char *name=part.queryProp("@name");
            const char *ptype=part.queryProp("@type");
            StringBuffer type;
            if (!strnicmp(ptype, "xsd:", 4))
            {
                type.append(translateXsdType(part.queryProp("@type")));
            }
            else
            {
                StringBuffer xpath;
                StringBuffer xname(name);
                xpath.appendf("Input[@name='%s']",xname.toLowerCase().str());
                if (xsdtree->hasProp(xpath.str()))
                    type.append("tns:t_").append(xname).append("Dataset");
                else
                    type.append(translateXsdType(part.queryProp("@type")));
            }

            schema.appendf("<xsd:element minOccurs=\"0\" maxOccurs=\"1\" name=\"%s\" type=\"%s\"", name, type.str());
            if (part.hasProp("@width") || part.hasProp("@height") || part.hasProp("@password") || part.hasProp("select"))
            {
                schema.append("><xsd:annotation><xsd:appinfo><form");
                unsigned rows = part.getPropInt("@height");
                if (rows)
                    schema.appendf(" formRows='%u'", rows);
                unsigned cols = part.getPropInt("@width");
                if (cols)
                    schema.appendf(" formCols='%u'", cols);
                if (part.hasProp("@password"))
                    schema.appendf(" password='%s'", part.queryProp("@password"));
                schema.appendf(">");
                if (part.hasProp("select"))
                    toXML(part.queryPropTree("select"), schema);
                schema.appendf("</form></xsd:appinfo></xsd:annotation></xsd:element>");
            }
            else
                schema.append("/>");
        }
    }
    schema.append("</xsd:all>");
    schema.append("</xsd:complexType>");
    schema.append("</xsd:element>");
}


int CWsEclBinding::getXsdDefinition(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda)
{
    WsEclWuInfo *wsinfo = (WsEclWuInfo *) context.getBindingValue();
    if (wsinfo)
        getXsdDefinition(context, request, content, *wsinfo);
    return 0;
}

int CWsEclBinding::getXsdDefinition(IEspContext &context, CHttpRequest *request, StringBuffer &content, WsEclWuInfo &wsinfo)
{
    IProperties *httpparms=request->queryParameters();

    if (wsecl)
    {
        StringBuffer xsds;
        wsinfo.getSchemas(xsds);
        Owned<IPropertyTree> xsdtree;
        if (xsds.length())
            xsdtree.setown(createPTreeFromXMLString(xsds.str()));

        //common types
        content.append(
            "<xsd:complexType name=\"EspStringArray\">"
                "<xsd:sequence>"
                    "<xsd:element name=\"Item\" type=\"xsd:string\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>"
                "</xsd:sequence>"
            "</xsd:complexType>"
            "<xsd:complexType name=\"EspIntArray\">"
                "<xsd:sequence>"
                    "<xsd:element name=\"Item\" type=\"xsd:int\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>"
                "</xsd:sequence>"
            "</xsd:complexType>"
            "<xsd:simpleType name=\"XmlDataSet\">"
                "<xsd:restriction base=\"xsd:string\"/>"
            "</xsd:simpleType>"
            "<xsd:simpleType name=\"CsvDataFile\">"
                "<xsd:restriction base=\"xsd:string\"/>"
            "</xsd:simpleType>"
            "<xsd:simpleType name=\"RawDataFile\">"
                "<xsd:restriction base=\"xsd:base64Binary\"/>"
            "</xsd:simpleType>");

        if (wsinfo.queryname.length()>0)
        {

            IPropertyTree *parmTree = wsinfo.queryParamInfo();
            if (xsdtree)
            {
                BoolHash added;
                Owned<IPropertyTreeIterator> input_xsds =xsdtree->getElements("Input");
                ForEach (*input_xsds)
                {
                    IPropertyTree &input = input_xsds->query();
                    VStringBuffer xpath("part[@name='%s']", input.queryProp("@name"));
                    if (parmTree->hasProp(xpath))
                        appendEclInputXsds(content, &input, added);
                }
            }
            SOAPSectionToXsd(wsinfo, parmTree, content, true, xsdtree);

            content.appendf("<xsd:element name=\"%sResponse\">", wsinfo.queryname.str());
            content.append("<xsd:complexType>");
            content.append("<xsd:all>");
            content.append("<xsd:element name=\"Exceptions\" type=\"tns:ArrayOfEspException\" minOccurs=\"0\"/>");
            content.append("<xsd:element name=\"Wuid\" type=\"xsd:string\" minOccurs=\"0\"/>");

            Owned<IPropertyTreeIterator> result_xsds =xsdtree->getElements("Result");
            if (!result_xsds->first())
            {
                content.append("<xsd:element name=\"Results\" type=\"xsd:string\" minOccurs=\"0\"/>");
            }
            else
            {
            content.append(
                "<xsd:element name=\"Results\" minOccurs=\"0\">"
                    "<xsd:complexType>"
                        "<xsd:all>"
                            "<xsd:element name=\"Result\">"
                                "<xsd:complexType>"
                                    "<xsd:sequence>"
                                       "<xsd:choice minOccurs='0' maxOccurs='unbounded'>"
                                          "<xsd:element name='Exception' type='tns:EspException'/>"
                                          "<xsd:element name='Info' type='tns:EspException'/>"
                                          "<xsd:element name='Warning' type='tns:EspException'/>"
                                          "<xsd:element name='Alert' type='tns:EspException'/>"
                                       "</xsd:choice>"
);
                int count=1;
                ForEach (*result_xsds)
                {
                    content.appendf("<xsd:element ref=\"ds%d:Dataset\" minOccurs=\"0\"/>", count++);
                }
                            content.append(
                                "</xsd:sequence>"
                                "</xsd:complexType>"
                            "</xsd:element>"
                        "</xsd:all>"
                    "</xsd:complexType>"
                "</xsd:element>");
            }

            content.append("</xsd:all>");
            content.append("<xsd:attribute name=\"sequence\" type=\"xsd:int\"/>");
            content.append("</xsd:complexType>");
            content.append("</xsd:element>");
        }
    }

    return 0;
}


bool CWsEclBinding::getSchema(StringBuffer& schema, IEspContext &ctx, CHttpRequest* req, WsEclWuInfo &wsinfo)
{
    Owned<IPropertyTree> namespaces = createPTree();
    appendSchemaNamespaces(namespaces, ctx, req, wsinfo);
    Owned<IPropertyTreeIterator> nsiter = namespaces->getElements("namespace");
    
    StringBuffer urn("urn:hpccsystems:ecl:");
    urn.appendLower(wsinfo.queryname.length(), wsinfo.queryname.str());
    schema.appendf("<xsd:schema elementFormDefault=\"qualified\" targetNamespace=\"%s\" ", urn.str());
    schema.appendf(" xmlns:tns=\"%s\"  xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"", urn.str());
    ForEach(*nsiter)
    {
        IPropertyTree &ns = nsiter->query();
        schema.appendf(" xmlns:%s=\"%s\"", ns.queryProp("@nsvar"), ns.queryProp("@ns"));
    }
    schema.append(">\n");
    ForEach(*nsiter)
    {
        IPropertyTree &ns = nsiter->query();
        if (ns.hasProp("@import"))
            schema.appendf("<xsd:import namespace=\"%s\" schemaLocation=\"%s\"/>", ns.queryProp("@ns"), ns.queryProp("@location"));
    }


    schema.append(
        "<xsd:complexType name=\"EspException\">"
            "<xsd:all>"
                "<xsd:element name=\"Code\" type=\"xsd:string\"  minOccurs=\"0\"/>"
                "<xsd:element name=\"Audience\" type=\"xsd:string\" minOccurs=\"0\"/>"
                "<xsd:element name=\"Source\" type=\"xsd:string\"  minOccurs=\"0\"/>"
                "<xsd:element name=\"Message\" type=\"xsd:string\" minOccurs=\"0\"/>"
            "</xsd:all>"
        "</xsd:complexType>\n"
        "<xsd:complexType name=\"ArrayOfEspException\">"
            "<xsd:sequence>"
                "<xsd:element name=\"Source\" type=\"xsd:string\"  minOccurs=\"0\"/>"
                "<xsd:element name=\"Exception\" type=\"tns:EspException\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>"
            "</xsd:sequence>"
        "</xsd:complexType>\n"
        "<xsd:element name=\"Exceptions\" type=\"tns:ArrayOfEspException\"/>\n"
    );

    getXsdDefinition(ctx, req, schema, wsinfo);

    schema.append("<xsd:element name=\"string\" nillable=\"true\" type=\"xsd:string\" />\n");
    schema.append("</xsd:schema>");

    return true;
}

int CWsEclBinding::getGenForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wuinfo, bool box)
{
    IConstWorkUnit *wu = wuinfo.ensureWorkUnit();
    IProperties *parms = request->queryParameters();

    StringBuffer page;
    Owned<IXslProcessor> xslp = getXslProcessor();

    StringBuffer v;
    StringBuffer formxml("<FormInfo>");
    appendXMLTag(formxml, "WUID", wuinfo.queryWuid());
    appendXMLTag(formxml, "QuerySet", wuinfo.qsetname.str());
    appendXMLTag(formxml, "QueryName", wuinfo.queryname.str());
    appendXMLTag(formxml, "ClientVersion", v.appendf("%g",context.getClientVersion()).str());
    appendXMLTag(formxml, "RequestElement", v.clear().append(wuinfo.queryname).append("Request").str());

    StringBuffer help;
    StringBuffer info;

    Owned<IConstWUWebServicesInfo> ws = wu->getWebServicesInfo();
    if (ws)
    {
        StringBufferAdaptor helpSv(help);
        StringBufferAdaptor infoSv(info);

        ws->getText("help", helpSv);
        ws->getText("description", infoSv);
    }

    Owned<IWuWebView> web = createWuWebView(*wu, wuinfo.qsetname.get(), wuinfo.queryname.get(), getCFD(), true, queryXsltConfig());
    if (web)
    {
        if (!help.length())
            web->aggregateResources("HELP", help);
        if (!info.length())
            web->aggregateResources("INFO", info);;
    }
    if (help.length())
        appendXMLTag(formxml, "Help", help.str());
    if (info.length())
        appendXMLTag(formxml, "Info", info.str());

    context.addOptions(ESPCTX_ALL_ANNOTATION);
    if (box)
    {
        StringBuffer xmlreq;
        getWsEcl2XmlRequest(xmlreq, context, request, wuinfo, "xml", NULL, 0, true);
        if (xmlreq.length())
        {
            Owned<IPropertyTree> pretty = createPTreeFromXMLString(xmlreq.str(), ipt_ordered);
            if (pretty)
            {
                toXML(pretty, xmlreq.clear());
                formxml.append("<Request>");
                encodeUtf8XML(xmlreq, formxml);
                formxml.append("</Request>");
            }
        }
    }
    else
        getSchema(formxml, context, request, wuinfo);

    formxml.append("<CustomViews>");
    if (web)
    {
        StringArray views;
        web->getResultViewNames(views);
        ForEachItemIn(i, views)
            appendXMLTag(formxml, "Result", views.item(i));
    }
    formxml.append("</CustomViews>");
    formxml.append("</FormInfo>");

    Owned<IXslTransform> xform = xslp->createXslTransform(queryXsltConfig());

    StringBuffer xslfile(getCFD());
    if (box)
        xslfile.append("./xslt/wsecl3_boxform.xsl");
    else
        xslfile.append("./xslt/wsecl3_form.xsl");

    xform->loadXslFromFile(xslfile.str());
    xform->setXmlSource(formxml.str(), formxml.length()+1);

    // pass params to form (excluding form and __querystring)
    StringBuffer params;
    if (!getUrlParams(context.queryRequestParameters(),params))
        params.appendf("%cver_=%g",(params.length()>0) ? '&' : '?', context.getClientVersion());
    xform->setStringParameter("queryParams", params.str());
    xform->setParameter("formOptionsAccess", "1");
    xform->setParameter("includeSoapTest", "1");
    xform->setParameter("useTextareaForStringArray", "1");

    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(wuinfo.qsetname);
    bool isRoxie = clusterInfo && (clusterInfo->getPlatform() == RoxieCluster);
    xform->setParameter("includeRoxieOptions", isRoxie ? "1" : "0");

    // set the prop noDefaultValue param
    IProperties* props = context.queryRequestParameters();
    bool formInitialized = false;
    if (props) {
        Owned<IPropertyIterator> it = props->getIterator();
        for (it->first(); it->isValid(); it->next()) {
            const char* key = it->getPropKey();
            if (*key=='.') {
                formInitialized = true;
                break;
            }
        }
    }
    xform->setParameter("noDefaultValue", formInitialized ? "1" : "0");

    xform->transform(page);
    response->setContentType("text/html");

    response->setContent(page.str());
    response->send();

    return 0;
}

inline void appendParameterNode(StringBuffer &xpath, StringBuffer &node)
{
    if (node.length())
    {
        if (isdigit(node.charAt(0)))
            xpath.setLength(xpath.length()-1);
        xpath.append(node);
        node.clear();
    }
}

void appendValidInputBoxContent(StringBuffer &xml, const char *in)
{
    //more later
    Owned<IPropertyTree> validAndFlat = createPTreeFromXMLString(in, ipt_ordered);
    toXML(validAndFlat, xml, 0, 0);
}

void CWsEclBinding::getWsEcl2XmlRequest(StringBuffer& soapmsg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo, const char *xmltype, const char *ns, unsigned flags, bool validate)
{
    IProperties *parameters = context.queryRequestParameters();
    const char *boxInput = parameters->queryProp("_boxFormInput");
    if (boxInput)
    {
        appendValidInputBoxContent(soapmsg, boxInput);
        return;
    }

    Owned<IPropertyTree> reqTree = createPTreeFromHttpParameters(wsinfo.queryname, parameters, true, false);

    if (!validate)
        toXML(reqTree, soapmsg, 0, 0);
    else
    {
        StringBuffer element;
        element.append(wsinfo.queryname.str()).append("Request");

        StringBuffer schemaXml;
        getSchema(schemaXml, context, request, wsinfo);
        ESPLOG(LogMax,"request schema: %s", schemaXml.str());
        Owned<IXmlSchema> schema = createXmlSchemaFromString(schemaXml);
        if (schema.get())
        {
            IXmlType* type = schema->queryElementType(element);
            if (type)
            {
                StringArray parentTypes;
                buildReqXml(parentTypes, type, soapmsg, (!stricmp(xmltype, "roxiexml")) ? wsinfo.queryname.str() : element.str(), reqTree, flags|REQSF_ROOT, ns);
            }
        }
    }
}

void CWsEclBinding::getWsEclJsonRequest(StringBuffer& jsonmsg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo, const char *xmltype, const char *ns, unsigned flags, bool validate)
{
    size32_t start = jsonmsg.length();
    try
    {
        IProperties *parameters = context.queryRequestParameters();
        Owned<IPropertyTree> reqTree = createPTreeFromHttpParameters(wsinfo.queryname, parameters, true, false);

        if (!validate)
        {
            jsonmsg.append('{');
            appendJSONName(jsonmsg, wsinfo.queryname);
            toJSON(reqTree, jsonmsg, 0, 0);
            jsonmsg.append('}');
            return;
        }
        StringBuffer element;
        element.append(wsinfo.queryname.str());
            element.append("Request");

        StringBuffer schemaXml;
        getSchema(schemaXml, context, request, wsinfo);
        ESPLOG(LogMax,"request schema: %s", schemaXml.str());
        Owned<IXmlSchema> schema = createXmlSchemaFromString(schemaXml);
        if (schema.get())
        {
            IXmlType* type = schema->queryElementType(element);
            if (type)
            {
                StringArray parentTypes;
                JsonHelpers::buildJsonMsg(parentTypes, type, jsonmsg, wsinfo.queryname.str(), reqTree, flags|REQSF_ROOT);
            }
        }
    }
    catch (IException *e)
    {
        jsonmsg.setLength(start);
        JsonHelpers::appendJSONException(jsonmsg.append('{'), e);
        jsonmsg.append('}');
    }
}

void CWsEclBinding::buildSampleResponseJSON(StringBuffer& msg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo)
{
    StringBuffer element;
    element.append(wsinfo.queryname.str()).append("Response");

    StringBuffer xsds;
    wsinfo.getSchemas(xsds);

    const char *jsonp = context.queryRequestParameters()->queryProp("jsonp");
    if (jsonp && *jsonp)
        msg.append(jsonp).append('(');
    msg.append('{');
    appendJSONName(msg, element);
    msg.append('{');
    appendJSONName(msg, "Results");
    msg.append('{');

    Owned<IPropertyTree> xsds_tree;
    if (xsds.length())
        xsds_tree.setown(createPTreeFromXMLString(xsds.str()));

    if (xsds_tree)
    {
        Owned<IPropertyTreeIterator> result_xsds =xsds_tree->getElements("Result");
        ForEach (*result_xsds)
            buildSampleJsonDataset(msg, result_xsds->query().queryPropTree("xs:schema"), wsinfo.qsetname.str(), wsinfo.queryname.str(), result_xsds->query().queryProp("@name"));
    }

    msg.append("}}}");
    if (jsonp && *jsonp)
        msg.append(");");
}

void CWsEclBinding::getSoapMessage(StringBuffer& soapmsg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo, unsigned flags, bool validate)
{
    soapmsg.append(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
          " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">"
            " <soap:Body>"
        );

    StringBuffer ns;
    ns.append("xmlns=\"urn:hpccsystems:ecl:").appendLower(wsinfo.queryname.length(), wsinfo.queryname.str()).append('\"');
    getWsEcl2XmlRequest(soapmsg, context, request, wsinfo, "soap", ns.str(), flags, validate);

    soapmsg.append("</soap:Body></soap:Envelope>");
}

int CWsEclBinding::getXmlTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *formtype, WsEclWuInfo &wsinfo)
{
    getXmlTestForm(context, request, response, wsinfo, formtype);
    return 0;
};

inline StringBuffer &buildWsEclTargetUrl(StringBuffer &url, WsEclWuInfo &wsinfo, bool createWorkunit, const char *type, const char *params)
{
    url.append("/WsEcl/").append(type);
    if (createWorkunit)
        url.append("run");
    url.append('/');
    if (wsinfo.qsetname.length() && wsinfo.queryname.length())
        url.append("query/").append(wsinfo.qsetname.get()).append('/').append(wsinfo.queryname.get());
    else
        url.append("wuid/").append(wsinfo.queryWuid());
    if (params && *params)
        url.append('?').append(params);
    return url;
}

int CWsEclBinding::getXmlTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, const char *formtype)
{
    IProperties *parms = context.queryRequestParameters();

    StringBuffer soapmsg, pageName;
    getSoapMessage(soapmsg, context, request, wsinfo, REQSF_TRIM|REQSF_ROOT, true);

    StringBuffer params;
    const char* excludes[] = {"soap_builder_",NULL};
    getEspUrlParams(context,params,excludes);

    Owned<IXslProcessor> xslp = getXslProcessor();
    Owned<IXslTransform> xform = xslp->createXslTransform(queryXsltConfig());
    xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/wsecl3_xmltest.xsl").str());

    StringBuffer srcxml;
    srcxml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><srcxml><soapbody><![CDATA[");
    srcxml.append(soapmsg.str());
    srcxml.append("]]></soapbody></srcxml>");
    xform->setXmlSource(srcxml.str(), srcxml.length());

    StringBuffer header;
    if (!stricmp(formtype, "roxiexml"))
    {
        header.append("Content-Type: application/xml; charset=UTF-8");
        xform->setStringParameter("showhttp", "true()");
        pageName.append("ROXIE XML Test");
    }
    else if (!stricmp(formtype, "roxiesoap"))
    {
        header.append("Content-Type: text/xml; charset=UTF-8");
        xform->setStringParameter("showhttp", "true()");
        pageName.append("ROXIE SOAP Test");
    }
    else
    {
        header.append("Content-Type: text/xml; charset=UTF-8");
        xform->setStringParameter("showhttp", "true()");
        pageName.append("SOAP Test");
    }

    // params
    xform->setStringParameter("pageName", pageName.str());
    xform->setStringParameter("serviceName", wsinfo.qsetname.str());
    xform->setStringParameter("methodName", wsinfo.queryname.str());
    xform->setStringParameter("wuid", wsinfo.queryWuid());
    xform->setStringParameter("header", header.str());

    ISecUser* user = context.queryUser();
    bool inhouse = user && (user->getStatus()==SecUserStatus_Inhouse);
    xform->setParameter("inhouseUser", inhouse ? "true()" : "false()");

    StringBuffer url;
    xform->setStringParameter("destination", buildWsEclTargetUrl(url, wsinfo, false, formtype, params.str()).str());

    bool isRoxieReq = wsecl->connMap.getValue(wsinfo.qsetname.get())!=NULL;
    if (isRoxieReq)
    {
        xform->setStringParameter("showJobType", "true()");
        xform->setStringParameter("createWorkunitDestination", buildWsEclTargetUrl(url.clear(), wsinfo, true, formtype, params.str()).str());
    }

    StringBuffer page;
    xform->transform(page);

    response->setContent(page);
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);
    response->send();

    return 0;
}

int CWsEclBinding::getJsonTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, const char *formtype)
{
    IProperties *parms = context.queryRequestParameters();

    StringBuffer jsonmsg, pageName;
    getWsEclJsonRequest(jsonmsg, context, request, wsinfo, "json", NULL, REQSF_TRIM, true);

    StringBuffer params;
    const char* excludes[] = {"soap_builder_",NULL};
    getEspUrlParams(context,params,excludes);

    StringBuffer header("Content-Type: application/json");

    Owned<IXslProcessor> xslp = getXslProcessor();
    Owned<IXslTransform> xform = xslp->createXslTransform(queryXsltConfig());
    xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/wsecl3_jsontest.xsl").str());

    StringBuffer encodedMsg;
    StringBuffer srcxml;
    srcxml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><srcxml><jsonreq><![CDATA[");
    srcxml.append(encodeJSON(encodedMsg, jsonmsg.str())); //encode the whole thing for javascript embedding
    srcxml.append("]]></jsonreq></srcxml>");
    xform->setXmlSource(srcxml.str(), srcxml.length());

    xform->setStringParameter("showhttp", "true()");
    pageName.append("JSON Test");

    // params
    xform->setStringParameter("pageName", pageName.str());
    xform->setStringParameter("serviceName", wsinfo.qsetname.str());
    xform->setStringParameter("methodName", wsinfo.queryname.str());
    xform->setStringParameter("wuid", wsinfo.queryWuid());
    xform->setStringParameter("header", header.str());

    ISecUser* user = context.queryUser();
    bool inhouse = user && (user->getStatus()==SecUserStatus_Inhouse);
    xform->setParameter("inhouseUser", inhouse ? "true()" : "false()");

    StringBuffer url;
    xform->setStringParameter("destination", buildWsEclTargetUrl(url, wsinfo, false, formtype, params.str()).str());

    bool isRoxieReq = wsecl->connMap.getValue(wsinfo.qsetname.get())!=NULL;
    if (isRoxieReq)
    {
        xform->setStringParameter("showJobType", "true()");
        xform->setStringParameter("createWorkunitDestination", buildWsEclTargetUrl(url, wsinfo, true, formtype, params.str()).str());
    }


    StringBuffer page;
    xform->transform(page);

    response->setContent(page);
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);
    response->send();

    return 0;
}

int CWsEclBinding::onGetSoapBuilder(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo)
{
    return getXmlTestForm(context, request, response, wsinfo, "soap");
}

bool checkWsEclFormType(StringBuffer &form, const char *value)
{
    if (value)
    {
        bool save = (strieq(value, "ecl")||strieq(value, "box"));
        if (save || (strieq(value, "soap")||strieq(value, "json")))
        {
            form.set(value);
            return save;
        }
    }
    form.set("ecl");
    return false;
}

void getWsEclFormType(CHttpRequest* request, CHttpResponse* response, StringBuffer &form)
{
    bool save=false;
    if (strieq(form, "default"))
    {
        CEspCookie *cookie = request->queryCookie("defaultWsEclForm");
        checkWsEclFormType(form, (cookie) ? cookie->getValue() : NULL);
    }
    else if (checkWsEclFormType(form, form.str()))
    {
        CEspCookie *cookie = request->queryCookie("defaultWsEclForm");
        if (!cookie || !strieq(cookie->getValue(), form.str()))
            response->addCookie(new CEspCookie("defaultWsEclForm", form));
    }
}

int CWsEclBinding::getWsEcl2Form(CHttpRequest* request, CHttpResponse* response, const char *thepath)
{
    IEspContext *context = request->queryContext();

    StringBuffer formtype;
    nextPathNode(thepath, formtype);

    StringBuffer wuid;
    StringBuffer qs;
    StringBuffer qid;
    splitLookupInfo(request->queryParameters(), thepath, wuid, qs, qid);
    WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());

    getWsEclFormType(request, response, formtype);
    if (strieq(formtype.str(), "ecl"))
        return getGenForm(*context, request, response, wsinfo, false);
    else if (strieq(formtype.str(), "box"))
        return getGenForm(*context, request, response, wsinfo, true);
    else if (strieq(formtype.str(), "soap"))
        return getXmlTestForm(*context, request, response, "soap", wsinfo);
    else if (strieq(formtype.str(), "json"))
        return getJsonTestForm(*context, request, response, wsinfo, "json");

    return 0;
}

int CWsEclBinding::submitWsEclWorkunit(IEspContext & context, WsEclWuInfo &wsinfo, IPropertyTree *reqTree, StringBuffer &out, unsigned flags, CHttpRequest *httpreq, TextMarkupFormat fmt, const char *viewname, const char *xsltname)
{
    IConstWorkUnit *sourceWorkUnit = wsinfo.ensureWorkUnit();

    Owned <IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned <IWorkUnit> workunit = factory->createWorkUnit("wsecl", context.queryUserId(), context.querySecManager(), context.queryUser());

    IExtendedWUInterface *ext = queryExtendedWU(workunit);
    ext->copyWorkUnit(sourceWorkUnit, false, false);

    workunit->clearExceptions();
    workunit->resetWorkflow();
    workunit->setClusterName(wsinfo.qsetname.str());
    workunit->setUser(context.queryUserId());

    const char *jobname = context.queryRequestParameters()->queryProp("_jobname");
    if (jobname && *jobname)
        workunit->setJobName(jobname);

    StringAttr wuid(workunit->queryWuid());  // NB queryWuid() not valid after workunit,clear()

    if (httpreq)
    {
        StringBuffer globalId, callerId;
        wsecl->getHttpGlobalIdHeader(httpreq, globalId);
        wsecl->getHttpCallerIdHeader(httpreq, callerId);
        if (globalId.length())
        {
            workunit->setDebugValue("GlobalId", globalId.str(), true);

            SocketEndpoint ep;
            StringBuffer localId;
            appendLocalId(localId, httpreq->getSocket()->getEndpoint(ep), 0);
            workunit->setDebugValue("CallerId", localId.str(), true); //our localId becomes caller id for the next hop
            DBGLOG("GlobalId: %s, CallerId: %s, LocalId: %s, Wuid: %s", globalId.str(), callerId.str(), localId.str(), wuid.str());
        }
    }

    workunit->setState(WUStateSubmitted);
    workunit->commit();

    if (reqTree)
    {
        if (reqTree->hasProp("Envelope"))
            reqTree=reqTree->queryPropTree("Envelope[1]");
        if (reqTree->hasProp("Body"))
            reqTree=reqTree->queryPropTree("Body[1]/*[1]");
        workunit->setXmlParams(LINK(reqTree));
    }

    workunit->schedule();
    workunit.clear();

    runWorkUnit(wuid.str(), wsinfo.qsetname.str());

    bool async = context.queryRequestParameters()->hasProp("_async");

    //don't wait indefinitely, in case submitted to an inactive queue wait max + 5 mins
    if (!async && waitForWorkUnitToComplete(wuid.str(), wsecl->workunitTimeout))
    {
        Owned<IWuWebView> web = createWuWebView(wuid.str(), wsinfo.qsetname.get(), wsinfo.queryname.get(), getCFD(), true, queryXsltConfig());
        if (!web)
        {
            DBGLOG("WS-ECL failed to create WuWebView for workunit %s", wuid.str());
            return 0;
        }
        if (viewname)
            web->renderResults(viewname, out);
        else if (xsltname)
            web->applyResultsXSLT(xsltname, out);
        else if (fmt==MarkupFmt_JSON)
            web->renderResultsJSON(out, context.queryRequestParameters()->queryProp("jsonp"));
        else
            web->expandResults(out, flags);
    }
    else
    {
        if (!async)
            DBGLOG("WS-ECL request timed out, WorkUnit %s", wuid.str());
        Owned<IWuWebView> web = createWuWebView(wuid.str(), wsinfo.qsetname.get(), wsinfo.queryname.get(), getCFD(), true, queryXsltConfig());
        web->createWuidResponse(out, flags);
    }

    DBGLOG("WS-ECL Request processed");
    return true;
}

int CWsEclBinding::submitWsEclWorkunit(IEspContext & context, WsEclWuInfo &wsinfo, const char *xml, StringBuffer &out, unsigned flags, CHttpRequest *httpreq, TextMarkupFormat fmt, const char *viewname, const char *xsltname)
{
    Owned<IPropertyTree> reqTree = createPTreeFromXMLString(xml, ipt_ordered, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces));
    return submitWsEclWorkunit(context, wsinfo, reqTree, out, flags, httpreq, fmt, viewname, xsltname);
}

void CWsEclBinding::sendRoxieRequest(const char *target, StringBuffer &req, StringBuffer &resp, StringBuffer &status, const char *query, bool trim, const char *contentType, CHttpRequest *httpreq)
{
    ISmartSocketFactory *conn = NULL;
    SocketEndpoint ep;
    try
    {
        ISmartSocketFactory *conn = wsecl->connMap.getValue(target);
        if (!conn)
            throw MakeStringException(-1, "roxie target cluster not mapped: %s", target);
        ep = conn->nextEndpoint();

        Owned<IHttpClientContext> httpctx = getHttpClientContext();
        StringBuffer url("http://");
        ep.getIpText(url).append(':').append(ep.port ? ep.port : 9876).append('/');
        RoxieSocketFactory *roxieConn = static_cast<RoxieSocketFactory*>(conn);
        if (roxieConn->includeTargetInURL)
            url.append(roxieConn->alias.isEmpty() ? target : roxieConn->alias.str());
        if (!trim)
            url.append("?.trim=0");

        Owned<IProperties> headers;
        Owned<IHttpClient> httpclient = httpctx->createHttpClient(NULL, url);
        httpclient->setTimeOut(wsecl->roxieTimeout);
        if (httpreq)
        {
            StringBuffer globalId, callerId;
            wsecl->getHttpGlobalIdHeader(httpreq, globalId);
            wsecl->getHttpCallerIdHeader(httpreq, callerId);

            if (globalId.length())
            {
                headers.setown(createProperties());
                headers->setProp(wsecl->queryGlobalIdHeaderName(), globalId);

                SocketEndpoint ep;
                StringBuffer localId;
                appendLocalId(localId, httpreq->getSocket()->getEndpoint(ep), 0);
                if (localId.length())
                    headers->setProp(wsecl->queryCallerIdHeaderName(), localId);
                DBGLOG("GlobalId: %s, CallerId: %s, LocalId: %s", globalId.str(), callerId.str(), localId.str());
            }
        }
        if (0 > httpclient->sendRequest("POST", contentType, req, resp, status))
            throw MakeStringException(-1, "Roxie cluster communication error: %s", target);
    }
    catch (IException *e)
    {
        if (conn && !ep.isNull())
            conn->setStatus(ep, false);

        StringBuffer s;
        if (strieq(contentType, "application/json"))
        {
            resp.set("{").append("\"").append(query).append("Response\": {\"Results\": {");
            JsonHelpers::appendJSONException(resp, e);
            resp.append("}}}");
        }
        else
        {
            VStringBuffer uri("urn:hpccsystems:ecl:%s", query);
            resp.set("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            resp.append("<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body>");
            resp.append('<').append(query).append("Response xmlns='").append(uri).append("'>");
            resp.append("<Results><Result><Exception><Source>WsEcl</Source>");
            resp.append("<Code>").append(e->errorCode()).append("</Code>");
            resp.append("<Message>").append(e->errorMessage(s)).append("</Message>");
            resp.append("</Exception></Result></Results>");
            resp.append("</").append(query).append("Response></soap:Body></soap:Envelope>");
        }
        e->Release();
    }
}

int CWsEclBinding::onSubmitQueryOutput(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, const char *format, bool forceCreateWorkunit)
{
    StringBuffer status;
    StringBuffer output;

    SCMStringBuffer clustertype;
    const char *contentType="application/xml";
    bool callRoxieQuery = !forceCreateWorkunit && wsecl->connMap.getValue(wsinfo.qsetname.get())!=NULL;
    bool outputJSON = (format && strieq(format, "json"));
    const char *jsonp = context.queryRequestParameters()->queryProp("jsonp");
    bool trim = context.queryRequestParameters()->getPropBool(".trim", true);
    bool trim2 = context.queryRequestParameters()->getPropBool("trim", true);
    if (callRoxieQuery && outputJSON)
    {
        StringBuffer jsonmsg;
        getWsEclJsonRequest(jsonmsg, context, request, wsinfo, "json", NULL, REQSF_TRIM, false);
        if (jsonp && *jsonp)
            output.append(jsonp).append('(');
        sendRoxieRequest(wsinfo.qsetname.get(), jsonmsg, output, status, wsinfo.queryname, trim, "application/json", request);
        if (jsonp && *jsonp)
            output.append(");");
    }
    else
    {
        StringBuffer soapmsg;
        getSoapMessage(soapmsg, context, request, wsinfo, REQSF_TRIM|REQSF_ROOT, false);
        if (getEspLogLevel()>LogNormal)
            DBGLOG("submitQuery soap: %s", soapmsg.str());

        unsigned xmlflags = WWV_ADD_RESPONSE_TAG | WWV_INCL_NAMESPACES | WWV_INCL_GENERATED_NAMESPACES;
        if (context.queryRequestParameters()->hasProp("display"))
            xmlflags |= WWV_USE_DISPLAY_XSLT;
        if (!format || !streq(format, "expanded"))
            xmlflags |= WWV_OMIT_SCHEMAS;
        if (!callRoxieQuery)
            submitWsEclWorkunit(context, wsinfo, soapmsg.str(), output, xmlflags, request, outputJSON ? MarkupFmt_JSON : MarkupFmt_XML);
        else
        {
            StringBuffer roxieresp;
            sendRoxieRequest(wsinfo.qsetname, soapmsg, roxieresp, status, wsinfo.queryname, trim, "text/xml", request);
            if (xmlflags & WWV_OMIT_SCHEMAS)
                expandWuXmlResults(output, wsinfo.queryname, roxieresp.str(), xmlflags);
            else
            {
                IConstWorkUnit *wu = wsinfo.ensureWorkUnit();
                Owned<IWuWebView> web = createWuWebView(*wu, wsinfo.qsetname.get(), wsinfo.queryname.get(), getCFD(), true, queryXsltConfig());
                if (web.get())
                    web->expandResults(roxieresp.str(), output, xmlflags);
            }

        }
    }

    if (outputJSON)
        contentType = (jsonp && *jsonp) ? "application/javascript" : "application/json";

    response->setContent(output.str());
    response->setContentType(contentType);
    response->setStatus("200 OK");
    response->send();

    return 0;
}

int CWsEclBinding::onSubmitQueryOutputView(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, bool forceCreateWorkunit)
{
    IConstWorkUnit *wu = wsinfo.ensureWorkUnit();

    StringBuffer soapmsg;
    getSoapMessage(soapmsg, context, request, wsinfo, REQSF_TRIM|REQSF_ROOT, false);
    if (getEspLogLevel()>LogNormal)
        DBGLOG("submitQuery soap: %s", soapmsg.str());

    const char *thepath = request->queryPath();

    StringBuffer output;
    StringBuffer status;
    StringBuffer html;

    SCMStringBuffer clustertype;
    wu->getDebugValue("targetclustertype", clustertype);

    StringBuffer xsltfile(getCFD());
    xsltfile.append("xslt/wsecl3_result.xslt");
    const char *view = context.queryRequestParameters()->queryProp("view");
    if (!forceCreateWorkunit && strieq(clustertype.str(), "roxie"))
    {
        sendRoxieRequest(wsinfo.qsetname.get(), soapmsg, output, status, wsinfo.queryname, false, "text/xml", request);
        Owned<IWuWebView> web = createWuWebView(*wu, wsinfo.qsetname.get(), wsinfo.queryname.get(), getCFD(), true, queryXsltConfig());
        if (!view)
            web->applyResultsXSLT(xsltfile.str(), output.str(), html);
        else
            web->renderResults(view, output.str(), html);
    }
    else
    {
        submitWsEclWorkunit(context, wsinfo, soapmsg.str(), html, 0, request, MarkupFmt_XML, view, xsltfile.str());
    }

    response->setContent(html.str());
    response->setContentType("text/html; charset=utf-8");
    response->setStatus("200 OK");
    response->send();

    return 0;
}


int CWsEclBinding::getWsdlMessages(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda)
{
    WsEclWuInfo *wsinfo = (WsEclWuInfo *) context.getBindingValue();
    if (wsinfo)
    {
        content.appendf("<message name=\"%sSoapIn\">", wsinfo->queryname.str());
        content.appendf("<part name=\"parameters\" element=\"tns:%sRequest\"/>", wsinfo->queryname.str());
        content.append("</message>");

        content.appendf("<message name=\"%sSoapOut\">", wsinfo->queryname.str());
        content.appendf("<part name=\"parameters\" element=\"tns:%sResponse\"/>", wsinfo->queryname.str());
        content.append("</message>");
    }

    return 0;
}

int CWsEclBinding::getWsdlPorts(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda)
{
    WsEclWuInfo *wsinfo = (WsEclWuInfo *) context.getBindingValue();
    if (wsinfo)
    {
        content.appendf("<portType name=\"%sServiceSoap\">", wsinfo->qsetname.str());
        content.appendf("<operation name=\"%s\">", wsinfo->queryname.str());
        content.appendf("<input message=\"tns:%sSoapIn\"/>", wsinfo->queryname.str());
        content.appendf("<output message=\"tns:%sSoapOut\"/>", wsinfo->queryname.str());
        content.append("</operation>");
        content.append("</portType>");
    }
    return 0;
}

int CWsEclBinding::getWsdlBindings(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda)
{
    WsEclWuInfo *wsinfo = (WsEclWuInfo *) context.getBindingValue();
    if (wsinfo)
    {
        content.appendf("<binding name=\"%sServiceSoap\" type=\"tns:%sServiceSoap\">", wsinfo->qsetname.str(), wsinfo->qsetname.str());
        content.append("<soap:binding transport=\"http://schemas.xmlsoap.org/soap/http\" style=\"document\"/>");

        content.appendf("<operation name=\"%s\">", wsinfo->queryname.str());
        content.appendf("<soap:operation soapAction=\"/%s/%s?ver_=1.0\" style=\"document\"/>", wsinfo->qsetname.str(), wsinfo->queryname.str());
        content.append("<input>");
        content.append("<soap:body use=\"literal\"/>");
        content.append("</input>");
        content.append("<output><soap:body use=\"literal\"/></output>");
        content.append("</operation>");
        content.append("</binding>");
    }

    return 0;
}


int CWsEclBinding::onGetWsdl(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo)
{
    context.setBindingValue(&wsinfo);
    EspHttpBinding::onGetWsdl(context, request, response, wsinfo.qsetname.str(), wsinfo.queryname.str());
    context.setBindingValue(NULL);
    return 0;
}

int CWsEclBinding::onGetXsd(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo)
{
    context.setBindingValue(&wsinfo);
    EspHttpBinding::onGetXsd(context, request, response, wsinfo.qsetname.str(), wsinfo.queryname.str());
    context.setBindingValue(NULL);

    return 0;
}


int CWsEclBinding::getWsEclDefinition(CHttpRequest* request, CHttpResponse* response, const char *thepath)
{
    IEspContext *context = request->queryContext();
    IProperties *parms = context->queryRequestParameters();

    StringBuffer wuid;
    StringBuffer qs;
    StringBuffer qid;
    splitLookupInfo(parms, thepath, wuid, qs, qid);
    WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());

    StringBuffer scope; //main, input, result, etc.
    nextPathNode(thepath, scope);

    StringBuffer respath;
    StringBuffer resname;
    StringBuffer restype;

    if (strieq(scope.str(), "resource"))
    {
        StringBuffer ext;
        splitPathTailAndExt(thepath, respath, resname, &ext);
    }
    else
        splitPathTailAndExt(thepath, respath, resname, &restype);

    if (strieq(scope.str(), "resource"))
    {
        StringBuffer blockStr("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        if (request->getParameters()->hasProp("display"))
            blockStr.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");

        wsinfo.getWsResource(respath.str(), blockStr);

        if (blockStr.length())
        {
            response->setStatus("200 OK");
            response->setContent(blockStr.str());
            response->setContentType(HTTP_TYPE_APPLICATION_XML);
            response->send();
        }
    }
    else if (strieq(restype.str(), "wsdl"))
    {
        if (strieq(scope.str(), "main"))
        {
            VStringBuffer dest("/WsEcl/soap/query/%s/%s", qs.str(), qid.str());
            parms->setProp("multiple_resp_schemas", 1);
            parms->setProp("wsdl_destination_path", dest.str());
            return onGetWsdl(*context, request, response, wsinfo);
        }
    }
    else if (strieq(restype.str(), "xsd"))
    {
        if (strieq(scope.str(), "main"))
        {
            parms->setProp("multiple_resp_schemas", 1);
            return onGetXsd(*context, request, response, wsinfo);
        }
        else if (strieq(scope.str(), "input") || strieq(scope.str(), "result"))
        {
            StringBuffer output;
            StringBuffer xsds;
            wsinfo.getSchemas(xsds);

            Owned<IPropertyTree> xsds_tree;
            if (xsds.length())
                xsds_tree.setown(createPTreeFromXMLString(xsds.str(), ipt_ordered));
            if (xsds_tree)
            {
                StringBuffer xpath;
                Owned<IPropertyTree> selected_xsd;
                StringBuffer urn("urn:hpccsystems:ecl:");
                appendNamespaceSpecificString(urn, wsinfo.queryname.get());
                urn.appendf(":%s:", scope.toLowerCase().str());
                appendNamespaceSpecificString(urn, resname.str());

                if (!stricmp(scope.str(), "input"))
                    xpath.appendf("Input[@sname='%s']/xs:schema", resname.str());
                else if (!stricmp(scope.str(), "result"))
                    xpath.appendf("Result[@sname='%s']/xs:schema",resname.str());
                if (xpath.length())
                    selected_xsd.setown(xsds_tree->getPropTree(xpath.str()));
                if (selected_xsd)
                {
                    selected_xsd->setProp("@targetNamespace", urn.str());
                    selected_xsd->setProp("@xmlns", urn.str());
                    IPropertyTree *dstree = selected_xsd->queryPropTree("xs:element[@name='Dataset']/xs:complexType");
                    if (dstree && !dstree->hasProp("xs:attribute[@name='name']"))
                        dstree->addPropTree("xs:attribute", createPTreeFromXMLString("<xs:attribute name=\"name\" type=\"xs:string\"/>"));
                    Owned<IPropertyTreeIterator> elements = selected_xsd->getElements("xs:element//xs:element");
                    ForEach(*elements)
                        elements->query().setPropInt("@minOccurs", 0);
                    output.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    if (context->queryRequestParameters()->hasProp("display"))
                        output.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");
                    toXML(selected_xsd, output);
                }
            }
            if (output.length())
            {
                response->setStatus("200 OK");
                response->setContent(output.str());
                response->setContentType(HTTP_TYPE_APPLICATION_XML);
                response->send();
            }
        }
    }
    return 0;
}

int CWsEclBinding::getRestURL(IEspContext *ctx, CHttpRequest *request, CHttpResponse *response, WsEclWuInfo &wsinfo, IProperties *parms)
{
    StringBuffer element(wsinfo.queryname);
    element.append("Request");

    StringBuffer schemaXml;

    getSchema(schemaXml, *ctx, request, wsinfo);
    Owned<IXmlSchema> schema = createXmlSchemaFromString(schemaXml);
    if (schema.get())
    {
        IXmlType* type = schema->queryElementType(element);
        if (type)
        {
            StringArray parentTypes;
            StringArray path;

            StringBuffer urlParams("?");
            buildRestURL(parentTypes, path, type, urlParams, NULL, 0);

            StringBuffer xml;
            appendXMLOpenTag(xml, "resturl");
            appendXMLTag(xml, "version", "3");
            appendXMLTag(xml, "target", wsinfo.qsetname);
            appendXMLTag(xml, "query", wsinfo.queryname);
            appendXMLTag(xml, "urlParams", urlParams);
            appendXMLCloseTag(xml, "resturl");

            Owned<IXslProcessor> xslp = getXslProcessor();
            Owned<IXslTransform> xform = xslp->createXslTransform(queryXsltConfig());
            xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/wsecl3_url.xslt").str());
            xform->setXmlSource(xml.str(), xml.length());

            StringBuffer page;
            xform->transform(page);

            response->setContent(page);
            response->setContentType("text/html; charset=UTF-8");
            response->setStatus(HTTP_STATUS_OK);
            response->send();
        }
    }
    return 0;
}

void setResponseFormatByName(IEspContext *ctx, const char *respFormat)
{
    if (!ctx)
        return;
    ESPSerializationFormat fmt = ESPSerializationANY;
    if (strieq(respFormat, "xml"))
        fmt = ESPSerializationXML;
    else if (strieq(respFormat, "json"))
        fmt = ESPSerializationJSON;
    ctx->setResponseFormat(fmt);
}

int CWsEclBinding::getWsEclExample(CHttpRequest* request, CHttpResponse* response, const char *thepath)
{
    IProperties *parms = request->queryParameters();
    IEspContext *context = request->queryContext();

    StringBuffer exampletype;
    nextPathNode(thepath, exampletype);

    StringBuffer wuid;
    StringBuffer qs;
    StringBuffer qid;
    splitLookupInfo(parms, thepath, wuid, qs, qid);

    StringBuffer format;
    nextPathNode(thepath, format);

    ESPSerializationFormat fmt = strieq(format, "json") ? ESPSerializationJSON : ESPSerializationXML;

    WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());

    context->setBindingValue(&wsinfo);

    StringBuffer output;
    const char *contentType = HTTP_TYPE_APPLICATION_XML;
    if (!stricmp(exampletype.str(), "request"))
    {
        if (fmt==ESPSerializationXML)
            return onGetReqSampleXml(*context, request, response, qs.str(), qid.str());

        getWsEclJsonRequest(output, *context, request, wsinfo, "json", NULL, REQSF_ROOT | REQSF_SAMPLE_DATA, true);
        contentType = HTTP_TYPE_TEXT_PLAIN;
    }
    else if (!stricmp(exampletype.str(), "response"))
    {
        if (fmt==ESPSerializationXML)
            buildSampleResponseXml(output, *context, request, wsinfo);
        else
        {
            buildSampleResponseJSON(output, *context, request, wsinfo);
            contentType = HTTP_TYPE_TEXT_PLAIN;
        }
    }
    else if (!stricmp(exampletype.str(), "url"))
        return getRestURL(context, request, response, wsinfo, parms);

    if (output.length())
    {
        response->setStatus("200 OK");
        response->setContent(output.str());
        response->setContentType(contentType);
        response->send();
    }
    return 0;
}

int CWsEclBinding::onGet(CHttpRequest* request, CHttpResponse* response)
{
    Owned<IMultiException> me = MakeMultiException("WsEcl");

    try
    {
        IEspContext *context = request->queryContext();
        IProperties *parms = request->queryParameters();

        context->ensureFeatureAccess(WSECL_ACCESS, SecAccess_Full, -1, WSECL_ACCESS_DENIED);

        const char *thepath = request->queryPath();

        StringBuffer serviceName;
        firstPathNode(thepath, serviceName);

        if (stricmp(serviceName.str(), "WsEcl"))
            return EspHttpBinding::onGet(request, response);

        StringBuffer methodName;
        nextPathNode(thepath, methodName);
        if (strieq(methodName, "async"))
        {
            parms->setProp("_async", 1);
            methodName.set("run");
        }

        if(strieq(methodName.str(), "res"))
        {
           MemoryBuffer mb;
           StringBuffer mimetype;
           getWuResourceByPath(thepath, mb, mimetype);

           response->setContent(mb.length(), mb.toByteArray());
           response->setContentType(mimetype.str());
           response->setStatus(HTTP_STATUS_OK);
           response->send();
           return 0;
        }
        if(strieq(methodName.str(), "resurls"))
        {
           StringBuffer content, fmt;
           getWuResourceUrlListByPath(thepath, fmt, content, "/WsEcl/");

           response->setContent(content.str());
           response->setContentType(strieq(fmt, "json") ? "application/json" : "text/xml");
           response->setStatus(HTTP_STATUS_OK);
           response->send();
           return 0;
        }
        if(strieq(methodName.str(), "manifest"))
        {
           StringBuffer mf;
           getWuManifestByPath(thepath, mf);

           response->setContent(mf.str());
           response->setContentType("text/xml");
           response->setStatus(HTTP_STATUS_OK);
           response->send();
           return 0;
        }
        if (!stricmp(methodName.str(), "tabview"))
        {
            return getWsEcl2TabView(request, response, thepath);
        }
        else if (!stricmp(methodName.str(), "forms"))
        {
            return getWsEcl2Form(request, response, thepath);
        }
        else if (!stricmp(methodName.str(), "proxy"))
        {
            context->addTraceSummaryValue(LogMin, "wseclMode", "proxy");

            StringBuffer wuid;
            StringBuffer target;
            StringBuffer qid;

            splitLookupInfo(parms, thepath, wuid, target, qid);

            StringBuffer format;
            nextPathNode(thepath, format);
            setResponseFormatByName(context, format);

            if (!wsecl->connMap.getValue(target.str()))
                throw MakeStringException(-1, "Target cluster not mapped to roxie process!");
            Owned<IPropertyTree> pt = createPTreeFromHttpParameters(qid.str(), parms, true, false);
            StringBuffer soapreq(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
                  " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">"
                    " <soap:Body>"
                );
            toXML(pt, soapreq);
            soapreq.append("</soap:Body></soap:Envelope>");
            StringBuffer output;
            StringBuffer status;
            if (getEspLogLevel()>LogNormal)
                DBGLOG("roxie req: %s", soapreq.str());
            sendRoxieRequest(target, soapreq, output, status, qid, parms->getPropBool(".trim", true), "text/xml", request);
            if (getEspLogLevel()>LogNormal)
                DBGLOG("roxie resp: %s", output.str());

            if (context->queryRequestParameters()->hasProp("display"))
            {
                unsigned pos = 0;
                const char *start = output.str();
                if (!strnicmp(start, "<?xml ", 6))
                {
                    const char *enddecl = strstr(start, "?>");
                    if (enddecl)
                        pos = enddecl - start + 2;
                }
                output.insert(pos, "<?xml-stylesheet type='text/xsl' href='/esp/xslt/xmlformatter.xsl' ?>");
            }

            response->setContent(output.str());
            response->setContentType("application/xml");
            response->setStatus("200 OK");
            response->send();
        }
        else if (strieq(methodName, "submit") || strieq(methodName, "run"))
        {
            context->addTraceSummaryValue(LogMin, "wseclMode", methodName);

            StringBuffer wuid;
            StringBuffer qs;
            StringBuffer qid;

            splitLookupInfo(parms, thepath, wuid, qs, qid);

            StringBuffer format;
            nextPathNode(thepath, format);
            setResponseFormatByName(context, format);

            WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());
            return onSubmitQueryOutput(*context, request, response, wsinfo, format.str(), strieq(methodName, "run"));
        }
        else if (strieq(methodName.str(), "xslt") || strieq(methodName, "runxslt"))
        {
            context->addTraceSummaryValue(LogMin, "wseclMode", "xslt");

            StringBuffer wuid;
            StringBuffer qs;
            StringBuffer qid;

            splitLookupInfo(parms, thepath, wuid, qs, qid);
            WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());

            return onSubmitQueryOutputView(*context, request, response, wsinfo, strieq(methodName, "runxslt"));
        }
        else if (!stricmp(methodName.str(), "example"))
        {
            return getWsEclExample(request, response, thepath);
        }
        else if (!stricmp(methodName.str(), "definitions"))
        {
            return getWsEclDefinition(request, response, thepath);
        }
        else if (!stricmp(methodName.str(), "links"))
        {
            StringBuffer wuid;
            StringBuffer qs;
            StringBuffer qid;

            splitLookupInfo(parms, thepath, wuid, qs, qid);
            WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());
            return getWsEclLinks(*context, request, response, wsinfo);
        }
        else if (strieq(methodName.str(), "soap"))
        {
            StringBuffer url;
            url.append("/WsEcl/forms/soap/").append(thepath);
            response->redirect(*request, url);
            return 0;
        }
        else if (strieq(methodName.str(), "json"))
        {
            StringBuffer url;
            url.append("/WsEcl/forms/json/").append(thepath);
            response->redirect(*request, url);
            return 0;
        }
    }
    catch (IMultiException* mex)
    {
        me->append(*mex);
        mex->Release();
    }
    catch (IException* e)
    {
        me->append(*e);
    }
    catch (...)
    {
        me->append(*MakeStringExceptionDirect(-1, "Unknown Exception"));
    }
    
    response->handleExceptions(getXslProcessor(), me, "WsEcl", "", StringBuffer(getCFD()).append("./smc_xslt/exceptions.xslt").str(), false);
    return 0;
}

void checkForXmlResponseName(StartTag &starttag, StringBuffer &respname, int &soaplevel)
{
    if (respname.length())
        return;

    switch(soaplevel)
    {
        case 0:
        {
            if (!stricmp(starttag.getLocalName(), "Envelope"))
                soaplevel=1;
            else
                respname.append(starttag.getLocalName());
            break;
        }
        case 1:
            if (!stricmp(starttag.getLocalName(), "Body"))
                soaplevel=2;
            break;
        case 2:
            respname.append(starttag.getLocalName());
        default:
            break;
    }

    int len=respname.length();
    if (len>8 && !stricmp(respname.str()+len-8, "Response"))
        respname.setLength(len-8);
}

void CWsEclBinding::handleJSONPost(CHttpRequest *request, CHttpResponse *response)
{
    IEspContext *ctx = request->queryContext();
    ctx->addTraceSummaryValue(LogMin, "wseclMode", "JSONPost");
    IProperties *parms = request->queryParameters();
    StringBuffer jsonresp;

    try
    {
        ctx->ensureFeatureAccess(WSECL_ACCESS, SecAccess_Full, -1, WSECL_ACCESS_DENIED);

        const char *thepath = request->queryPath();

        StringBuffer serviceName;
        firstPathNode(thepath, serviceName);

        if (!strieq(serviceName.str(), "WsEcl"))
            EspHttpBinding::handleHttpPost(request, response);

        StringBuffer action;
        nextPathNode(thepath, action);

        bool forceCreateWorkunit = false;
        if (strieq(action, "async"))
            parms->setProp("_async", 1);
        else if (strieq(action, "soaprun"))
            forceCreateWorkunit = true;
        else if (strieq(action, "asyncrun"))
        {
            parms->setProp("_async", 1);
            forceCreateWorkunit = true;
        }


        StringBuffer lookup;
        nextPathNode(thepath, lookup);

        StringBuffer wuid;
        StringBuffer queryset;
        StringBuffer queryname;

        if (strieq(lookup.str(), "wuid"))
        {
            nextPathNode(thepath, wuid);
            queryset.append(parms->queryProp("qset"));
            queryname.append(parms->queryProp("qname"));
        }
        else if (strieq(lookup.str(), "query"))
        {
            nextPathNode(thepath, queryset);
            nextPathNode(thepath, queryname);
        }

        bool trim = ctx->queryRequestParameters()->getPropBool(".trim", true);
        const char *jsonp = ctx->queryRequestParameters()->queryProp("jsonp");
        if (jsonp && *jsonp)
            jsonresp.append(jsonp).append('(');

        StringBuffer content(request->queryContent());
        if (getEspLogLevel()>LogNormal)
            DBGLOG("json request: %s", content.str());

        StringBuffer status;
        if (!forceCreateWorkunit && wsecl->connMap.getValue(queryset.str()))
            sendRoxieRequest(queryset.str(), content, jsonresp, status, queryname.str(), trim, "application/json", request);
        else
        {
            WsEclWuInfo wsinfo(wuid.str(), queryset.str(), queryname.str(), ctx->queryUserId(), ctx->queryPassword());
            Owned<IPropertyTree> contentTree = createPTreeFromJSONString(content.str());
            IPropertyTree *reqTree = contentTree.get();

            StringBuffer fullname(queryname);
            fullname.append("Request");
            Owned<IPropertyTreeIterator> it = reqTree->getElements("*");
            ForEach(*it)
            {
                const char *name = it->query().queryName();
                if (strieq(name, queryname) || strieq(name, fullname))
                {
                    reqTree = &it->query();
                    break;
                }
            }
            submitWsEclWorkunit(*ctx, wsinfo, reqTree, jsonresp, 0, request, MarkupFmt_JSON);
        }
        if (jsonp && *jsonp)
            jsonresp.append(");");
        if (getEspLogLevel()>LogNormal)
            DBGLOG("json response: %s", jsonresp.str());

    }
    catch (IException *e)
    {
        JsonHelpers::appendJSONException(jsonresp.set("{"), e);
        jsonresp.append('}');
    }

    response->setContent(jsonresp.str());
    response->setContentType("application/json");
    response->setStatus("200 OK");
    response->send();
}

void CWsEclBinding::handleHttpPost(CHttpRequest *request, CHttpResponse *response)
{
    StringBuffer ct;
    request->getContentType(ct);
    if (!strnicmp(ct.str(), "application/json", 16))
    {
        handleJSONPost(request, response);
    }
    else
    {
        EspHttpBinding::handleHttpPost(request, response);
    }
}

int CWsEclBinding::HandleSoapRequest(CHttpRequest* request, CHttpResponse* response)
{
    IEspContext *ctx = request->queryContext();
    ctx->addTraceSummaryValue(LogMin, "wseclMode", "SOAPPost");
    IProperties *parms = request->queryParameters();

    const char *thepath = request->queryPath();

    StringBuffer serviceName;
    firstPathNode(thepath, serviceName);

    if (!strieq(serviceName.str(), "WsEcl"))
        return CHttpSoapBinding::HandleSoapRequest(request, response);

    if(ctx->toBeAuthenticated()) //future support WsSecurity tags?
    {
        ctx->setAuthStatus(AUTH_STATUS_FAIL);
        response->sendBasicChallenge(getChallengeRealm(), false);
        return 0;
    }

    ctx->ensureFeatureAccess(WSECL_ACCESS, SecAccess_Full, -1, WSECL_ACCESS_DENIED);

    StringBuffer action;
    nextPathNode(thepath, action);

    bool forceCreateWorkunit = false;
    if (strieq(action, "async"))
        parms->setProp("_async", 1);
    else if (strieq(action, "soaprun"))
        forceCreateWorkunit = true;
    else if (strieq(action, "asyncrun"))
    {
        parms->setProp("_async", 1);
        forceCreateWorkunit = true;
    }

    StringBuffer lookup;
    nextPathNode(thepath, lookup);

    StringBuffer wuid;
    StringBuffer target;
    StringBuffer queryname;

    if (strieq(lookup.str(), "wuid"))
    {
        nextPathNode(thepath, wuid);
        target.append(parms->queryProp("qset"));
        queryname.append(parms->queryProp("qname"));
    }
    else if (strieq(lookup.str(), "query"))
    {
        nextPathNode(thepath, target);
        nextPathNode(thepath, queryname);
    }

    StringBuffer content(request->queryContent());
    StringBuffer soapresp;
    StringBuffer status;

    unsigned xmlflags = WWV_ADD_SOAP | WWV_ADD_RESULTS_TAG | WWV_ADD_RESPONSE_TAG | WWV_INCL_NAMESPACES | WWV_INCL_GENERATED_NAMESPACES;
    if (ctx->queryRequestParameters()->hasProp("display"))
        xmlflags |= WWV_USE_DISPLAY_XSLT;
    if (streq(action.str(), "expanded"))
        xmlflags |= WWV_CDATA_SCHEMAS;
    else
        xmlflags |= WWV_OMIT_SCHEMAS;

    if (!forceCreateWorkunit && wsecl->connMap.getValue(target))
    {
        bool trim = ctx->queryRequestParameters()->getPropBool(".trim", true);
        StringBuffer content(request->queryContent());
        StringBuffer output;
        sendRoxieRequest(target, content, output, status, queryname, trim, "text/xml", request);
        if (!(xmlflags  & WWV_CDATA_SCHEMAS))
            soapresp.swapWith(output);
        else
        {
            if (xmlflags & WWV_OMIT_SCHEMAS)
                expandWuXmlResults(soapresp, queryname.str(), output.str(), xmlflags);
            else
            {
                WsEclWuInfo wsinfo(wuid.str(), target.str(), queryname.str(), ctx->queryUserId(), ctx->queryPassword());
                Owned<IWuWebView> web = createWuWebView(*wsinfo.ensureWorkUnit(), wsinfo.qsetname.get(), wsinfo.queryname.get(), getCFD(), true, queryXsltConfig());
                if (web.get())
                    web->expandResults(output.str(), soapresp, xmlflags);
            }
        }
    }
    else
    {
        WsEclWuInfo wsinfo(wuid.str(), target.str(), queryname.str(), ctx->queryUserId(), ctx->queryPassword());
        submitWsEclWorkunit(*ctx, wsinfo, content.str(), soapresp, xmlflags, request);
    }

    if (getEspLogLevel()>LogNormal)
        DBGLOG("HandleSoapRequest response: %s", soapresp.str());

    response->setContent(soapresp.str());
    response->setContentType("text/xml");
    response->setStatus("200 OK");
    response->send();

    return 0;
}
