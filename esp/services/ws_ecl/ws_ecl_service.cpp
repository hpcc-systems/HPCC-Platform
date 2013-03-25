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
#include "xpp/XmlPullParser.h"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5mins, 30s a bit short

#define     WSECL_ACCESS      "WsEclAccess"

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

int strptrcmp(char const ** l, char const ** r) { return strcmp(*l, *r); }

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
    portal_URL.set(prc->queryProp("@protalurl"));

    StringBuffer daliAddress;
    const char *daliServers = prc->queryProp("@daliServers");
    if (daliServers)
    {
        while (*daliServers && !strchr(":;,", *daliServers))
            daliAddress.append(*daliServers++);
    }

    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> environment = factory->openEnvironmentByFile();
    Owned<IPropertyTree> pRoot = &environment->getPTree();

    xpath.clear().appendf("EspService[@name='%s']/VIPS", name);
    IPropertyTree *vips = prc->queryPropTree(xpath.str());

    Owned<IPropertyTreeIterator> it = pRoot->getElements("Software/RoxieCluster");
    ForEach(*it)
    {
        const char *name = it->query().queryProp("@name");
        if (connMap.getValue(name)) //bad config?
            continue;
        bool loadBalanced = false;
        StringBuffer list;
        const char *vip = NULL;
        if (vips)
            vip = vips->queryProp(xpath.clear().appendf("ProcessCluster[@name='%s']/@vip", name).str());
        if (vip && *vip)
        {
            list.append(vip);
            loadBalanced = true;
        }
        else
        {
            Owned<IPropertyTreeIterator> servers = it->query().getElements("RoxieServerProcess");
            ForEach(*servers)
                appendServerAddress(list, *pRoot, servers->query(), daliAddress.str());
        }
        if (list.length())
        {
            Owned<ISmartSocketFactory> sf = createSmartSocketFactory(list.str(), !loadBalanced);
            connMap.setValue(name, sf.get());
        }
    }

    translator = new wsEclTypeTranslator();
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

IPropertyTree * getQueryRegistries()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/QuerySets/", myProcessSession(), RTM_LOCK_READ|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    return conn->getRoot();
}


void CWsEclBinding::getRootNavigationFolders(IEspContext &context, IPropertyTree & data)
{
    DBGLOG("CScrubbedXmlBinding::getNavigationData");

    StringArray wsModules;
    StringBuffer mode;

    data.addProp("@viewType", "wsecl_tree");
    data.addProp("@action", "NavMenuEvent");
    data.addProp("@appName", "WsECL 3.0");

    Owned<IStringIterator> targets = getTargetClusters(NULL, NULL);

    SCMStringBuffer target;
    ForEach(*targets)
    {
        VStringBuffer parms("queryset=%s", targets->str(target).str());
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
    if (query->getPropBool("@isLibrary") || query->getPropBool("@suspended"))
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
                qnames.sort(strptrcmp);
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


static inline bool isPathSeparator(char sep)
{
    return (sep=='\\')||(sep=='/');
}

static inline const char *skipPathNodes(const char *&s, int skip)
{
    if (s) {
        while (*s) {
            if (isPathSeparator(*s++))
                if (!skip--)
                    return s;
        }
    }
    return NULL;
}

static inline const char *nextPathNode(const char *&s, StringBuffer &node, int skip=0)
{
    if (skip)
        skipPathNodes(s, skip);
    if (s) while (*s) {
        if (isPathSeparator(*s))
            return s++;
        node.append(*s++);
    }
    return NULL;
}

static inline const char *firstPathNode(const char *&s, StringBuffer &node)
{
    if (s && isPathSeparator(*s))
        s++;
    return nextPathNode(s, node);
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
    Owned<IXslTransform> trans = proc->createXslTransform();

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


#define REQXML_ROOT         0x0001
#define REQXML_SAMPLE_DATA  0x0002
#define REQXML_TRIM         0x0004
#define REQXML_ESCAPEFORMATTERS 0x0008

static void buildReqXml(StringStack& parent, IXmlType* type, StringBuffer& out, const char* tag, IPropertyTree *parmtree, unsigned flags, const char* ns=NULL)
{
    assertex(type!=NULL);

    if (!parmtree && (flags & REQXML_TRIM) && !(flags & REQXML_ROOT))
        return;

    const char* typeName = type->queryName();
    if (type->isComplexType())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
            return; // recursive

        int startlen = out.length();
        out.appendf("<%s", tag);
        if (ns)
            out.append(' ').append(ns);
        int taglen=out.length()+1;
        for (size_t i=0; i<type->getAttrCount(); i++)
        {
            IXmlAttribute* attr = type->queryAttr(i);
            if (parmtree)
            {
                StringBuffer attrpath("@");
                const char *attrval = parmtree->queryProp(attrpath.append(attr->queryName()).str());
                if (attrval)
                    out.appendf(" %s='", attr->queryName()).append(attrval);
            }
            else
            {
                out.appendf(" %s='", attr->queryName());
                attr->getSampleValue(out);
            }
            out.append('\'');
        }
        out.append('>');
        if (typeName)
            parent.push_back(typeName);

        int flds = type->getFieldCount();
        switch (type->getSubType())
        {
        case SubType_Complex_SimpleContent:
            assertex(flds==0);
            if (parmtree)
            {
                const char *attrval = parmtree->queryProp(NULL);
                if (attrval)
                    out.append(attrval);
            }
            else if (flags & REQXML_SAMPLE_DATA)
                type->queryFieldType(0)->getSampleValue(out,tag);
            break;

        default:
            for (int idx=0; idx<flds; idx++)
            {
                IPropertyTree *childtree = NULL;
                const char *childname = type->queryFieldName(idx);
                if (parmtree)
                    childtree = parmtree->queryPropTree(childname);
                buildReqXml(parent,type->queryFieldType(idx), out, childname, childtree, flags & ~REQXML_ROOT);
            }
            break;
        }

        if (typeName)
            parent.pop_back();
        if ((flags & REQXML_TRIM) && !(flags & REQXML_ROOT) && out.length()==taglen)
            out.setLength(startlen);
        else
            out.appendf("</%s>",tag);
    }
    else if (type->isArray())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
            return; // recursive

        const char* itemName = type->queryFieldName(0);
        IXmlType*   itemType = type->queryFieldType(0);
        if (!itemName || !itemType)
            throw MakeStringException(-1,"*** Invalid array definition: tag=%s, itemName=%s", tag, itemName?itemName:"NULL");

        if (typeName)
            parent.push_back(typeName);

        int startlen = out.length();
        out.appendf("<%s", tag);
        if (ns)
            out.append(' ').append(ns);
        out.append(">");
        int taglen=out.length();
        if (parmtree)
        {
            VStringBuffer countpath("%s/itemcount", itemName);
            const char *countstr=parmtree->queryProp(countpath.str());
            if (countstr && *countstr)
            {
                int count = atoi(countstr);
                for (int idx=0; idx<count; idx++)
                {
                    StringBuffer itempath;
                    itempath.append(itemName).append(idx);
                    IPropertyTree *itemtree = parmtree->queryPropTree(itempath.str());
                    if (itemtree)
                        buildReqXml(parent,itemType,out,itemName, itemtree, flags & ~REQXML_ROOT);
                }
            }
            else if (parmtree->hasProp(itemName))
            {
                Owned<IPropertyTreeIterator> items = parmtree->getElements(itemName);
                ForEach(*items)
                    buildReqXml(parent,itemType,out,itemName, &items->query(), flags & ~REQXML_ROOT);
            }
            else
            {
                const char *s = parmtree->queryProp(NULL);
                if (s && *s)
                {
                    StringArray items;
                    items.appendList(s, "\n");
                    ForEachItemIn(i, items)
                        appendXMLTag(out, itemName, items.item(i));
                }

            }
        }
        else
            buildReqXml(parent,itemType,out,itemName, NULL, flags & ~REQXML_ROOT);

        if (typeName)
            parent.pop_back();
        if ((flags & REQXML_TRIM) && !(flags & REQXML_ROOT) && out.length()==taglen)
            out.setLength(startlen);
        else
            out.appendf("</%s>",tag);
    }
    else // simple type
    {
        StringBuffer parmval;
        if (parmtree)
            parmval.append(parmtree->queryProp(NULL));
        if (!parmval.length() && (flags & REQXML_SAMPLE_DATA))
            type->getSampleValue(parmval, NULL);
        
        if (parmval.length() || !(flags&REQXML_TRIM))
        {
            if (strieq(typeName, "boolean"))
            {
                if (!strieq(parmval, "default"))
                {
                    out.appendf("<%s>", tag);
                    if (parmval.length())
                        out.append((strieq(parmval.str(),"1")||strieq(parmval.str(),"true")||strieq(parmval.str(), "on")) ? '1' : '0');
                    out.appendf("</%s>", tag);
                }
            }
            else
            {
                out.appendf("<%s>", tag);
                out.append(parmval);
                out.appendf("</%s>", tag);
            }
        }
    }
}

inline void indenter(StringBuffer &s, int count)
{
    s.appendN(count*3, ' ');
}

IException *MakeJSONValueException(int code, const char *start, const char *pos, const char *tail, const char *intro="Invalid json format: ")
{
     StringBuffer s(intro);
     s.append(pos-start, start).append('^').append(pos);
     if (tail && *tail)
         s.append(" - ").append(tail);
     return MakeStringException(code, "%s", s.str());
}

inline StringBuffer &jsonNumericNext(StringBuffer &s, const char *&c, bool &allowDecimal, bool &allowExponent, const char *start)
{
    if (isdigit(*c))
        s.append(*c++);
    else if ('.'==*c)
    {
        if (!allowDecimal || !allowExponent)
            throw MakeJSONValueException(-1, start, c, "Unexpected decimal");
        allowDecimal=false;
        s.append(*c++);
    }
    else if ('e'==*c || 'E'==*c)
    {
        if (!allowExponent)
            throw MakeJSONValueException(-1, start, c, "Unexpected exponent");

        allowDecimal=false;
        allowExponent=false;
        s.append(*c++);
        if ('-'==*c || '+'==*c)
            s.append(*c++);
        if (!isdigit(*c))
            throw MakeJSONValueException(-1, start, c, "Unexpected token");
    }
    else
        throw MakeJSONValueException(-1, start, c, "Unexpected token");

    return s;
}

inline StringBuffer &jsonNumericStart(StringBuffer &s, const char *&c, const char *start)
{
    if ('-'==*c)
        return jsonNumericStart(s.append(*c++), c, start);
    else if ('0'==*c)
    {
        s.append(*c++);
        if (*c && '.'!=*c)
            throw MakeJSONValueException(-1, start, c, "Unexpected token");
    }
    else if (isdigit(*c))
        s.append(*c++);
    else
        throw MakeJSONValueException(-1, start, c, "Unexpected token");
    return s;
}

StringBuffer &appendJSONNumericString(StringBuffer &s, const char *value, bool allowDecimal)
{
    if (!value || !*value)
        return s.append("null");

    bool allowExponent = allowDecimal;

    const char *pos = value;
    jsonNumericStart(s, pos, value);
    while (*pos)
        jsonNumericNext(s, pos, allowDecimal, allowExponent, value);
    return s;
}

inline const char *jsonNewline(unsigned flags){return ((flags & REQXML_ESCAPEFORMATTERS) ? "\\n" : "\n");}

typedef enum _JSONFieldCategory
{
    JSONField_String,
    JSONField_Integer,
    JSONField_Real,
    JSONField_Boolean
} JSONField_Category;

JSONField_Category xsdTypeToJSONFieldCategory(const char *xsdtype)
{
    if (!strnicmp(xsdtype, "real", 4) || !strnicmp(xsdtype, "dec", 3) || !strnicmp(xsdtype, "double", 6) || !strnicmp(xsdtype, "float", 5))
        return JSONField_Real;
    if (!strnicmp(xsdtype, "int", 3))
        return JSONField_Integer;
    if (!strnicmp(xsdtype, "bool", 4))
        return JSONField_Boolean;
    return JSONField_String;
}

static void buildJsonAppendValue(StringStack& parent, IXmlType* type, StringBuffer& out, const char* tag, const char *value, unsigned flags, int &indent)
{
    indenter(out, indent);
    if (tag && *tag)
        out.appendf("\"%s\": ", tag);
    StringBuffer sample;
    if ((!value || !*value) && (flags & REQXML_SAMPLE_DATA))
    {
        type->getSampleValue(sample, NULL);
        value = sample.str();
    }

    if (value)
    {
        switch (xsdTypeToJSONFieldCategory(type->queryName()))
        {
        case JSONField_String:
            appendJSONValue(out, NULL, value);
            break;
        case JSONField_Integer:
            appendJSONNumericString(out, value, false);
            break;
        case JSONField_Real:
            appendJSONNumericString(out, value, true);
            break;
        case JSONField_Boolean:
            appendJSONValue(out, NULL, (bool)('1'==*value || strieq(value, "true")));
            break;
        }
    }
    else
        out.append("null");
}

static void buildJsonMsg(StringStack& parent, IXmlType* type, StringBuffer& out, const char* tag, IPropertyTree *parmtree, unsigned flags, int &indent)
{
    assertex(type!=NULL);

    if (flags & REQXML_ROOT)
    {
        out.append("{");
        out.append(jsonNewline(flags));
        indent++;
    }

    const char* typeName = type->queryName();
    if (type->isComplexType())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
            return; // recursive

        int startlen = out.length();
        indenter(out, indent++);
        if (tag)
            out.appendf("\"%s\": {", tag).append(jsonNewline(flags));
        else
            out.append("{").append(jsonNewline(flags));
        int taglen=out.length()+1;
        if (typeName)
            parent.push_back(typeName);
        if (type->getSubType()==SubType_Complex_SimpleContent)
        {
            if (parmtree)
            {
                const char *attrval = parmtree->queryProp(NULL);
                indenter(out, indent);
                out.appendf("\"%s\" ", (attrval) ? attrval : "");
            }
            else if (flags & REQXML_SAMPLE_DATA)
            {
                indenter(out, indent);
                out.append("\"");
                type->queryFieldType(0)->getSampleValue(out,tag);
                out.append("\" ");
            }
        }
        else
        {
            bool first=true;
            int flds = type->getFieldCount();
            for (int idx=0; idx<flds; idx++)
            {
                if (first)
                    first=false;
                else
                    out.append(",").append(jsonNewline(flags));
                IPropertyTree *childtree = NULL;
                const char *childname = type->queryFieldName(idx);
                if (parmtree)
                    childtree = parmtree->queryPropTree(childname);
                buildJsonMsg(parent, type->queryFieldType(idx), out, childname, childtree, flags & ~REQXML_ROOT, indent);
            }
            out.append(jsonNewline(flags));
        }

        if (typeName)
            parent.pop_back();
        indenter(out, indent--);
        out.append("}");
    }
    else if (type->isArray())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
            return; // recursive

        const char* itemName = type->queryFieldName(0);
        IXmlType*   itemType = type->queryFieldType(0);
        if (!itemName || !itemType)
            throw MakeStringException(-1,"*** Invalid array definition: tag=%s, itemName=%s", tag, itemName?itemName:"NULL");

        if (typeName)
            parent.push_back(typeName);

        int startlen = out.length();
        indenter(out, indent++);
        if (tag)
            out.appendf("\"%s\": {%s", tag, jsonNewline(flags));
        else
            out.append("{").append(jsonNewline(flags));
        indenter(out, indent++);
        out.appendf("\"%s\": [", itemName).append(jsonNewline(flags));
        indent++;
        int taglen=out.length();
        if (parmtree)
        {
            VStringBuffer countpath("%s/itemcount", itemName);
            const char *countstr=parmtree->queryProp(countpath.str());
            if (countstr && *countstr)
            {
                bool first=true;
                int count = atoi(countstr);
                for (int idx=0; idx<count; idx++)
                {
                    if (first)
                        first=false;
                    else
                        out.append(",").append(jsonNewline(flags));
                    StringBuffer itempath;
                    itempath.append(itemName).append(idx);
                    IPropertyTree *itemtree = parmtree->queryPropTree(itempath.str());
                    if (itemtree)
                        buildJsonMsg(parent,itemType,out, NULL, itemtree, flags & ~REQXML_ROOT, indent);
                }
                out.append(jsonNewline(flags));
            }
            else if (parmtree->hasProp(itemName))
            {
                Owned<IPropertyTreeIterator> items = parmtree->getElements(itemName);
                bool first=true;
                ForEach(*items)
                {
                    if (first)
                        first=false;
                    else
                        out.append(",").append(jsonNewline(flags));
                    buildJsonMsg(parent,itemType,out, NULL, &items->query(), flags & ~REQXML_ROOT, indent);
                }
                out.append(jsonNewline(flags));
            }
            else
            {
                const char *s = parmtree->queryProp(NULL);
                if (s && *s)
                {
                    StringArray items;
                    items.appendList(s, "\n");
                    ForEachItemIn(i, items)
                    {
                        delimitJSON(out, true, 0!=(flags & REQXML_ESCAPEFORMATTERS));
                        buildJsonAppendValue(parent, type, out, NULL, items.item(i), flags & ~REQXML_ROOT, indent);
                    }
                    out.append(jsonNewline(flags));
                }

            }
        }
        else
            buildJsonMsg(parent, itemType, out, NULL, NULL, flags & ~REQXML_ROOT, indent);

        indenter(out, indent--);
        out.append("]").append(jsonNewline(flags));

        if (typeName)
            parent.pop_back();
        indenter(out, indent--);
        out.append("}");
    }
    else // simple type
    {
        const char *parmval = (parmtree) ? parmtree->queryProp(NULL) : NULL;
        buildJsonAppendValue(parent, type, out, tag, parmval, flags, indent);
    }

    if (flags & REQXML_ROOT)
        out.append(jsonNewline(flags)).append("}");

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
            StringStack parent;
            buildReqXml(parent, type, xml, "Dataset", NULL, REQXML_SAMPLE_DATA, ns.str());
        }
    }

}

void CWsEclBinding::buildSampleResponseXml(StringBuffer& msg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo)
{
    StringBuffer element;
    element.append(wsinfo.queryname.sget()).append("Response");

    StringBuffer xsds;
    wsinfo.getSchemas(xsds);

    msg.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    if (context.queryRequestParameters()->hasProp("display"))
        msg.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");

    msg.append('<').append(element.str()).append(" xmlns=\"urn:hpccsystems:ecl:");
    msg.appendLower(wsinfo.queryname.length(), wsinfo.queryname.sget()).append("\">");
    msg.append("<Results><Result>");

    Owned<IPropertyTree> xsds_tree;
    if (xsds.length())
        xsds_tree.setown(createPTreeFromXMLString(xsds.str()));

    if (xsds_tree)
    {
        Owned<IPropertyTreeIterator> result_xsds =xsds_tree->getElements("Result");
        ForEach (*result_xsds)
            buildSampleDataset(msg, result_xsds->query().queryPropTree("xs:schema"), wsinfo.qsetname.sget(), wsinfo.queryname.sget(), result_xsds->query().queryProp("@name"));
    }

    msg.append("</Result></Results>");
    msg.append("</").append(element.str()).append('>');
}


int CWsEclBinding::getWsEclLinks(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo)
{
    StringBuffer xml;
    xml.append("<links>");
    xml.append("<version>3</version>");
    xml.append("<path>").append(wsinfo.qsetname.sget()).append("</path>");
    xml.append("<query>").append(wsinfo.queryname.sget()).append("</query>");

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
    Owned<IXslTransform> xform = xslp->createXslTransform();
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

    StringBuffer xml;
    xml.append("<tabview>");
    xml.append("<version>3</version>");
    xml.appendf("<wuid>%s</wuid>", wsinfo.wuid.sget());
    xml.appendf("<qset>%s</qset>", wsinfo.qsetname.sget());
    xml.appendf("<qname>%s</qname>", wsinfo.queryname.sget());

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
    //while (indent--)
    //  content.append('\t');
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
                    IPropertyTreeIterator *children = item.getElements("xs:complexType/xs:sequence/*");
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

void CWsEclBinding::SOAPSectionToXsd(WsEclWuInfo &wsinfo, const char *parmXml, StringBuffer &schema, bool isRequest, IPropertyTree *xsdtree)
{
    Owned<IPropertyTree> tree = createPTreeFromXMLString(parmXml, ipt_none, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_noRoot));

    schema.appendf("<xsd:element name=\"%s%s\">", wsinfo.queryname.sget(), isRequest ? "Request" : "Response");
    schema.append("<xsd:complexType>");
    schema.append("<xsd:all>");
    Owned<IPropertyTreeIterator> parts = tree->getElements("part");
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
            if (strieq(type.str(), "tns:XmlDataSet"))
            {
                schema.append(">"
                        "<xsd:annotation><xsd:appinfo>"
                            "<form formRows=\"25\" formCols=\"60\"/>"
                        "</xsd:appinfo></xsd:annotation>"
                    "</xsd:element>");
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
            StringBuffer parmXml;
            if (wsinfo.getWsResource("SOAP", parmXml))
            {
                if (xsdtree)
                {
                    BoolHash added;
                    Owned<IPropertyTreeIterator> input_xsds =xsdtree->getElements("Input");
                    ForEach (*input_xsds)
                    {
                        appendEclInputXsds(content, &input_xsds->query(), added);
                    }
                }
                SOAPSectionToXsd(wsinfo, parmXml.str(), content, true, xsdtree);
            }

            content.appendf("<xsd:element name=\"%sResponse\">", wsinfo.queryname.sget());
            content.append("<xsd:complexType>");
            content.append("<xsd:all>");
            content.append("<xsd:element name=\"Exceptions\" type=\"tns:ArrayOfEspException\" minOccurs=\"0\"/>");

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
                                    "<xsd:all>");
                int count=1;
                ForEach (*result_xsds)
                {
                    content.appendf("<xsd:element ref=\"ds%d:Dataset\" minOccurs=\"0\"/>", count++);
                }
                            content.append(
                                "</xsd:all>"
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
    urn.appendLower(wsinfo.queryname.length(), wsinfo.queryname.sget());
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

int CWsEclBinding::getGenForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, bool box)
{
    IProperties *parms = request->queryParameters();

    StringBuffer page;
    Owned<IXslProcessor> xslp = getXslProcessor();

    StringBuffer v;
    StringBuffer formxml("<FormInfo>");
    appendXMLTag(formxml, "WUID", wsinfo.wuid.sget());
    appendXMLTag(formxml, "QuerySet", wsinfo.qsetname.sget());
    appendXMLTag(formxml, "QueryName", wsinfo.queryname.sget());
    appendXMLTag(formxml, "ClientVersion", v.appendf("%g",context.getClientVersion()).str());
    appendXMLTag(formxml, "RequestElement", v.clear().append(wsinfo.queryname).append("Request").str());

    Owned<IWuWebView> web = createWuWebView(*wsinfo.wu.get(), wsinfo.queryname.get(), getCFD(), true);
    if (web)
    {
        appendXMLTag(formxml, "Help", web->aggregateResources("HELP", v.clear()).str());
        appendXMLTag(formxml, "Info", web->aggregateResources("INFO", v.clear()).str());
    }

    context.addOptions(ESPCTX_ALL_ANNOTATION);
    if (box)
    {
        StringBuffer xmlreq;
        getWsEcl2XmlRequest(xmlreq, context, request, wsinfo, "xml", NULL, 0);
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
        getSchema(formxml, context, request, wsinfo);

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

    Owned<IXslTransform> xform = xslp->createXslTransform();

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

void buildParametersXml(IPropertyTree *parmtree, IProperties *parms)
{
    Owned<IPropertyIterator> it = parms->getIterator();
    ForEach(*it)
    {
        const char *key = it->getPropKey();
        const char *val = parms->queryProp(key);
        StringBuffer xpath;
        if (key && *key && val && *val)
        {
            bool isidx=false;
            StringBuffer node;
            for (int pos=0; key[pos]!=0; pos++)
            {
                if (key[pos]!='.')
                    node.append(key[pos]);
                else
                {
                    appendParameterNode(xpath, node);
                    xpath.append('/');
                }
            }
            appendParameterNode(xpath, node);

            ensurePTree(parmtree, xpath.str());
            parmtree->setProp(xpath.str(), val);
        }
    }
    StringBuffer xml;
    toXML(parmtree, xml);
    DBGLOG("parmtree: %s", xml.str());
}

void appendValidInputBoxContent(StringBuffer &xml, const char *in)
{
    //more later
    Owned<IPropertyTree> validAndFlat = createPTreeFromXMLString(in, ipt_ordered);
    toXML(validAndFlat, xml, 0, 0);
}

void CWsEclBinding::getWsEcl2XmlRequest(StringBuffer& soapmsg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo, const char *xmltype, const char *ns, unsigned flags)
{
    Owned<IPropertyTree> parmtree = createPTree();
    IProperties *parms = context.queryRequestParameters();

    const char *boxInput = parms->queryProp("_boxFormInput");
    if (boxInput)
    {
        appendValidInputBoxContent(soapmsg, boxInput);
        return;
    }

    buildParametersXml(parmtree, parms);

    StringBuffer element;
    element.append(wsinfo.queryname.sget());
        element.append("Request");

    StringBuffer schemaXml;
    getSchema(schemaXml, context, request, wsinfo);
    DBGLOG("request schema: %s", schemaXml.str());
    Owned<IXmlSchema> schema = createXmlSchemaFromString(schemaXml);
    if (schema.get())
    {
        IXmlType* type = schema->queryElementType(element);
        if (type)
        {
            StringStack parent;
            buildReqXml(parent, type, soapmsg, (!stricmp(xmltype, "roxiexml")) ? wsinfo.queryname.sget() : element.str(), parmtree, flags|REQXML_ROOT, ns);
        }
    }
}

StringBuffer &appendJSONException(StringBuffer &s, IException *e, const char *objname="Exceptions", const char *arrayName = "Exception")
{
    if (!e)
        return s;
    if (objname && *objname)
        appendJSONName(s, objname).append('{');
    if (arrayName && *arrayName)
        appendJSONName(s, arrayName).append('[');
    delimitJSON(s);
    s.append('{');
    appendJSONValue(s, "Code", e->errorCode());
    StringBuffer temp;
    appendJSONValue(s, "Message", e->errorMessage(temp).str());
    s.append('}');
    if (arrayName && *arrayName)
        s.append(']');
    if (objname && *objname)
        s.append('}');
    return s;
}

StringBuffer &appendJSONExceptions(StringBuffer &s, IMultiException *e, const char *objname="Exceptions", const char *arrayName = "Exception")
{
    if (!e)
        return s;
    if (objname && *objname)
        appendJSONName(s, objname).append('{');
    if (arrayName && *arrayName)
        appendJSONName(s, arrayName).append('[');
    ForEachItemIn(i, *e)
        appendJSONException(s, &e->item(i), NULL, NULL);
    if (arrayName && *arrayName)
        s.append(']');
    if (objname && *objname)
        s.append('}');
    return s;
}

void CWsEclBinding::getWsEclJsonRequest(StringBuffer& jsonmsg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo, const char *xmltype, const char *ns, unsigned flags)
{
    size32_t start = jsonmsg.length();
    try
    {
        Owned<IPropertyTree> parmtree = createPTree();
        IProperties *parms = context.queryRequestParameters();

        buildParametersXml(parmtree, parms);

        StringBuffer element;
        element.append(wsinfo.queryname.sget());
            element.append("Request");

        StringBuffer schemaXml;
        getSchema(schemaXml, context, request, wsinfo);
        DBGLOG("request schema: %s", schemaXml.str());
        Owned<IXmlSchema> schema = createXmlSchemaFromString(schemaXml);
        if (schema.get())
        {
            IXmlType* type = schema->queryElementType(element);
            if (type)
            {
                StringStack parent;
                int indent=0;
                buildJsonMsg(parent, type, jsonmsg, wsinfo.queryname.sget(), parmtree, flags|REQXML_ROOT|REQXML_ESCAPEFORMATTERS, indent);
            }
        }
    }
    catch (IException *e)
    {
        jsonmsg.setLength(start);
        appendJSONException(jsonmsg.append('{'), e);
        jsonmsg.append('}');
    }
}

void CWsEclBinding::getWsEclJsonResponse(StringBuffer& jsonmsg, IEspContext &context, CHttpRequest *request, const char *xml, WsEclWuInfo &wsinfo)
{
    size32_t start = jsonmsg.length();
    try
    {
        Owned<IPropertyTree> parmtree = createPTreeFromXMLString(xml, ipt_none, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces));

        StringBuffer element;
        element.append(wsinfo.queryname.sget());
        element.append("Response");

        VStringBuffer xpath("Body/%s/Results/Result/Exception", element.str());
        Owned<IPropertyTreeIterator> exceptions = parmtree->getElements(xpath.str());

        jsonmsg.appendf("{\n  \"%s\": {\n    \"Results\": {\n", element.str());

        if (exceptions && exceptions->first())
        {
            jsonmsg.append("      \"Exceptions\": {\n        \"Exception\": [\n");
            bool first=true;
            ForEach(*exceptions)
            {
                if (first)
                    first=false;
                else
                    jsonmsg.append(",\n");
            jsonmsg.appendf("          {\n            \"Code\": %d,\n            \"Message\": \"%s\"\n          }", exceptions->query().getPropInt("Code"), exceptions->query().queryProp("Message"));
            }
            jsonmsg.append("\n        ]\n      }\n");
        }

        xpath.clear().append("Body/*[1]/Results/Result/Dataset");
        Owned<IPropertyTreeIterator> datasets = parmtree->getElements(xpath.str());

        ForEach(*datasets)
        {
            IPropertyTree &ds = datasets->query();
            const char *dsname = ds.queryProp("@name");
            if (dsname && *dsname)
            {
                StringBuffer schemaResult;
                wsinfo.getOutputSchema(schemaResult, dsname);
                if (schemaResult.length())
                {
                    Owned<IXmlSchema> schema = createXmlSchemaFromString(schemaResult);
                    if (schema.get())
                    {
                        IXmlType* type = schema->queryElementType("Dataset");
                        if (type)
                        {
                            StringStack parent;
                            int indent=4;
                            StringBuffer outname(dsname);
                            buildJsonMsg(parent, type, jsonmsg, outname.replace(' ', '_').str(), &ds, 0, indent);
                        }
                    }
                }
            }
        }

        jsonmsg.append("    }\n  }\n}");
    }
    catch (IException *e)
    {
        jsonmsg.setLength(start);
        appendJSONException(jsonmsg.append('{'), e);
        jsonmsg.append('}');
    }
}


void CWsEclBinding::getSoapMessage(StringBuffer& soapmsg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo, unsigned flags)
{
    soapmsg.append(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
          " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">"
            " <soap:Body>"
        );

    StringBuffer ns;
    ns.append("xmlns=\"urn:hpccsystems:ecl:").appendLower(wsinfo.queryname.length(), wsinfo.queryname.sget()).append('\"');
    getWsEcl2XmlRequest(soapmsg, context, request, wsinfo, "soap", ns.str(), flags);

    soapmsg.append("</soap:Body></soap:Envelope>");
}

int CWsEclBinding::getXmlTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *formtype, WsEclWuInfo &wsinfo)
{
    getXmlTestForm(context, request, response, wsinfo, formtype);
    return 0;
};

inline StringBuffer &buildWsEclTargetUrl(StringBuffer &url, WsEclWuInfo &wsinfo, const char *type, const char *params)
{
    url.append("/WsEcl/").append(type).append('/');
    if (wsinfo.qsetname.length() && wsinfo.queryname.length())
        url.append("query/").append(wsinfo.qsetname.get()).append('/').append(wsinfo.queryname.get());
    else
        url.append("wuid/").append(wsinfo.wuid.sget());
    if (params && *params)
        url.append('?').append(params);
    return url;
}

int CWsEclBinding::getXmlTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, const char *formtype)
{
    IProperties *parms = context.queryRequestParameters();

    StringBuffer soapmsg, pageName;
    getSoapMessage(soapmsg, context, request, wsinfo, 0);

    StringBuffer params;
    const char* excludes[] = {"soap_builder_",NULL};
    getEspUrlParams(context,params,excludes);

    Owned<IXslProcessor> xslp = getXslProcessor();
    Owned<IXslTransform> xform = xslp->createXslTransform();
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
    xform->setStringParameter("serviceName", wsinfo.qsetname.sget());
    xform->setStringParameter("methodName", wsinfo.queryname.sget());
    xform->setStringParameter("wuid", wsinfo.wuid.sget());
    xform->setStringParameter("header", header.str());

    ISecUser* user = context.queryUser();
    bool inhouse = user && (user->getStatus()==SecUserStatus_Inhouse);
    xform->setParameter("inhouseUser", inhouse ? "true()" : "false()");

    StringBuffer url;
    xform->setStringParameter("destination", buildWsEclTargetUrl(url, wsinfo, formtype, params.str()).str());

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
    getWsEclJsonRequest(jsonmsg, context, request, wsinfo, "json", NULL, 0);

    StringBuffer params;
    const char* excludes[] = {"soap_builder_",NULL};
    getEspUrlParams(context,params,excludes);

    StringBuffer header("Content-Type: application/json; charset=UTF-8");

    Owned<IXslProcessor> xslp = getXslProcessor();
    Owned<IXslTransform> xform = xslp->createXslTransform();
    xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/wsecl3_jsontest.xsl").str());

    StringBuffer srcxml;
    srcxml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><srcxml><jsonreq><![CDATA[");
    srcxml.append(jsonmsg.str());
    srcxml.append("]]></jsonreq></srcxml>");
    xform->setXmlSource(srcxml.str(), srcxml.length());

    xform->setStringParameter("showhttp", "true()");
    pageName.append("JSON Test");

    // params
    xform->setStringParameter("pageName", pageName.str());
    xform->setStringParameter("serviceName", wsinfo.qsetname.sget());
    xform->setStringParameter("methodName", wsinfo.queryname.sget());
    xform->setStringParameter("wuid", wsinfo.wuid.sget());
    xform->setStringParameter("header", header.str());

    ISecUser* user = context.queryUser();
    bool inhouse = user && (user->getStatus()==SecUserStatus_Inhouse);
    xform->setParameter("inhouseUser", inhouse ? "true()" : "false()");

    StringBuffer url;
    xform->setStringParameter("destination", buildWsEclTargetUrl(url, wsinfo, formtype, params.str()).str());

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

void CWsEclBinding::addParameterToWorkunit(IWorkUnit * workunit, IConstWUResult &vardef, IResultSetMetaData &metadef, const char *varname, IPropertyTree *valtree)
{
    if (!varname || !*varname)
        return;

    Owned<IWUResult> var = workunit->updateVariableByName(varname);
    if (!vardef.isResultScalar())
    {
        StringBuffer ds;
        if (valtree->hasChildren())
            toXML(valtree, ds);
        else
        {
            const char *val = valtree->queryProp(NULL);
            if (val)
                decodeXML(val, ds);
        }
        if (ds.length())
            var->setResultRaw(ds.length(), ds.str(), ResultFormatXml);
    }
    else
    {
        const char *val = valtree->queryProp(NULL);
        if (val && *val)
        {
            switch (metadef.getColumnDisplayType(0))
            {
                case TypeBoolean:
                    var->setResultBool(strieq(val, "1") || strieq(val, "true") || strieq(val, "on"));
                    break;
                case TypeInteger:
                    var->setResultInt(_atoi64(val));
                    break;
                case TypeUnsignedInteger:
                    var->setResultInt(_atoi64(val));
                    break;
                case TypeReal:
                    var->setResultReal(atof(val));
                    break;
                case TypeSet:
                case TypeDataset:
                case TypeData:
                    var->setResultRaw(strlen(val), val, ResultFormatRaw);
                    break;
                case TypeUnicode: {
                    MemoryBuffer target;
                    convertUtf(target, UtfReader::Utf16le, strlen(val), val, UtfReader::Utf8);
                    var->setResultUnicode(target.toByteArray(), (target.length()>1) ? target.length()/2 : 0);
                    }
                    break;
                case TypeString:
                case TypeUnknown:
                default:
                    var->setResultString(val, strlen(val));
                    break;
                    break;
            }

            var->setResultStatus(ResultStatusSupplied);
        }
    }
}


int CWsEclBinding::submitWsEclWorkunit(IEspContext & context, WsEclWuInfo &wsinfo, const char *xml, StringBuffer &out, unsigned flags, const char *viewname, const char *xsltname)
{
    Owned <IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
    Owned <IWorkUnit> workunit = factory->createWorkUnit(NULL, "wsecl", context.queryUserId());

    IExtendedWUInterface *ext = queryExtendedWU(workunit);
    ext->copyWorkUnit(wsinfo.wu, false);

    workunit->clearExceptions();
    workunit->resetWorkflow();
    workunit->setClusterName(wsinfo.qsetname.sget());
    workunit->setUser(context.queryUserId());
    
    SCMStringBuffer wuid;
    workunit->getWuid(wuid);

    SCMStringBuffer token;
    createToken(wuid.str(), context.queryUserId(), context.queryPassword(), token);
    workunit->setSecurityToken(token.str());
    workunit->setState(WUStateSubmitted);
    workunit->commit();

    Owned<IPropertyTree> req = createPTreeFromXMLString(xml, ipt_none, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces));
    IPropertyTree *start = req.get();
    if (start->hasProp("Envelope"))
        start=start->queryPropTree("Envelope");
    if (start->hasProp("Body"))
        start=start->queryPropTree("Body/*[1]");

    Owned<IResultSetFactory> resultSetFactory(getResultSetFactory(context.queryUserId(), context.queryPassword()));
    Owned<IPropertyTreeIterator> it = start->getElements("*");
    ForEach(*it)
    {
        IPropertyTree &eclparm=it->query();
        const char *varname = eclparm.queryName();

        IConstWUResult *vardef = wsinfo.wu->getVariableByName(varname);
        if (vardef)
        {
            Owned<IResultSetMetaData> metadef = resultSetFactory->createResultSetMeta(vardef);
            if (metadef)
                addParameterToWorkunit(workunit.get(), *vardef, *metadef, varname, &eclparm);
        }
    }

    workunit->schedule();
    workunit.clear();

    runWorkUnit(wuid.str(), wsinfo.qsetname.sget());

    //don't wait indefinately, in case submitted to an inactive queue wait max + 5 mins
    int wutimeout = 300000;
    if (waitForWorkUnitToComplete(wuid.str(), wutimeout))
    {
        Owned<IWuWebView> web = createWuWebView(wuid.str(), wsinfo.queryname.get(), getCFD(), true);
        if (!web)
        {
            DBGLOG("WS-ECL failed to create WuWebView for workunit %s", wuid.str());
            return 0;
        }
        if (viewname)
            web->renderResults(viewname, out);
        else if (xsltname)
            web->applyResultsXSLT(xsltname, out);
        else
            web->expandResults(out, flags);
    }
    else
    {
        DBGLOG("WS-ECL request timed out, WorkUnit %s", wuid.str());
    }

    DBGLOG("WS-ECL Request processed [using Doxie]");
    return true;
}

void xppToXmlString(XmlPullParser &xpp, StartTag &stag, StringBuffer &buffer)
{
    int level = 1; //assumed due to the way gotonextdataset works.
    int type = XmlPullParser::END_TAG;
    const char * content = "";
    const char *tag = NULL;
    EndTag etag;

    tag = stag.getLocalName();
    if (tag && *tag)
    {
        buffer.appendf("<%s", tag);
        for (int idx=0; idx<stag.getLength(); idx++)
            buffer.appendf(" %s=\"%s\"", stag.getRawName(idx), stag.getValue(idx));
        buffer.append(">");
    }

    do  
    {
        type = xpp.next();
        switch(type) 
        {
            case XmlPullParser::START_TAG:
            {
                xpp.readStartTag(stag);
                ++level;
                tag = stag.getLocalName();
                if (tag && *tag)
                {
                    buffer.appendf("<%s", tag);
                    for (int idx=0; idx<stag.getLength(); idx++)
                        buffer.appendf(" %s=\"%s\"", stag.getRawName(idx), stag.getValue(idx));
                    buffer.append(">");
                }
                break;
            }
            case XmlPullParser::END_TAG:
                xpp.readEndTag(etag);
                tag = etag.getLocalName();
                if (tag && *tag)
                    buffer.appendf("</%s>", tag);
                --level;
            break;
            case XmlPullParser::CONTENT:
                content = xpp.readContent();
                encodeUtf8XML(content, buffer);
                break;
            case XmlPullParser::END_DOCUMENT:
                level=0;
            break;
        }
    }
    while (level > 0);
}

bool xppGotoTag(XmlPullParser &xppx, const char *tagname, StartTag &stag)
{
    int level = 0;
    int type = XmlPullParser::END_TAG;
    do  
    {
        type = xppx.next();
        switch(type) 
        {
            case XmlPullParser::START_TAG:
            {
                xppx.readStartTag(stag);
                ++level;
                const char *tag = stag.getLocalName();
                if (tag && strieq(tag, tagname))
                    return true;
                break;
            }
            case XmlPullParser::END_TAG:
                --level;
            break;
            case XmlPullParser::END_DOCUMENT:
                level=0;
            break;
        }
    }
    while (level > 0);
    return false;
}

void CWsEclBinding::sendRoxieRequest(const char *target, StringBuffer &req, StringBuffer &resp, StringBuffer &status, const char *query, const char *contentType)
{
    ISmartSocketFactory *conn = NULL;
    SocketEndpoint ep;
    try
    {
        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(target);
        if (!clusterInfo)
            throw MakeStringException(-1, "target cluster not found");

        SCMStringBuffer process;
        clusterInfo->getRoxieProcess(process);
        ISmartSocketFactory *conn = wsecl->connMap.getValue(process.str());
        if (!conn)
            throw MakeStringException(-1, "process cluster not found: %s", process.str());

        ep = conn->nextEndpoint();

        Owned<IHttpClientContext> httpctx = getHttpClientContext();
        StringBuffer url("http://");
        ep.getIpText(url).append(':').append(ep.port);

        Owned<IHttpClient> httpclient = httpctx->createHttpClient(NULL, url);
        if (0 > httpclient->sendRequest("POST", contentType, req, resp, status))
            throw MakeStringException(-1, "Process cluster communication error: %s", process.str());
    }
    catch (IException *e)
    {
        if (conn && !ep.isNull())
            conn->setStatus(ep, false);

        StringBuffer s;
        if (strieq(contentType, "application/json"))
        {
            resp.set("{").append("\"").append(query).append("Response\": {\"Results\": {");
            appendJSONException(resp, e);
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

int CWsEclBinding::onSubmitQueryOutputXML(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, const char *format)
{
    StringBuffer soapmsg;

    getSoapMessage(soapmsg, context, request, wsinfo, REQXML_TRIM|REQXML_ROOT);
    DBGLOG("submitQuery soap: %s", soapmsg.str());

    const char *thepath = request->queryPath();

    StringBuffer status;
    StringBuffer output;

    SCMStringBuffer clustertype;
    wsinfo.wu->getDebugValue("targetclustertype", clustertype);

    unsigned xmlflags = WWV_ADD_RESPONSE_TAG | WWV_INCL_NAMESPACES | WWV_INCL_GENERATED_NAMESPACES;
    if (context.queryRequestParameters()->hasProp("display"))
        xmlflags |= WWV_USE_DISPLAY_XSLT;
    if (!format || !streq(format, "expanded"))
        xmlflags |= WWV_OMIT_SCHEMAS;
    if (strieq(clustertype.str(), "roxie"))
    {
        StringBuffer roxieresp;
        sendRoxieRequest(wsinfo.qsetname.get(), soapmsg, roxieresp, status, wsinfo.queryname);

        Owned<IWuWebView> web = createWuWebView(*wsinfo.wu, wsinfo.queryname.get(), getCFD(), true);
        if (web.get())
            web->expandResults(roxieresp.str(), output, xmlflags);
    }
    else
    {
        submitWsEclWorkunit(context, wsinfo, soapmsg.str(), output, xmlflags);
    }

    response->setContent(output.str());
    response->setContentType(HTTP_TYPE_APPLICATION_XML);
    response->setStatus("200 OK");
    response->send();

    return 0;
}

int CWsEclBinding::onSubmitQueryOutputView(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo)
{
    StringBuffer soapmsg;

    getSoapMessage(soapmsg, context, request, wsinfo, REQXML_TRIM|REQXML_ROOT);
    DBGLOG("submitQuery soap: %s", soapmsg.str());

    const char *thepath = request->queryPath();

    StringBuffer output;
    StringBuffer status;
    StringBuffer html;

    SCMStringBuffer clustertype;
    wsinfo.wu->getDebugValue("targetclustertype", clustertype);

    StringBuffer xsltfile(getCFD());
    xsltfile.append("xslt/wsecl3_result.xslt");
    const char *view = context.queryRequestParameters()->queryProp("view");
    if (strieq(clustertype.str(), "roxie"))
    {
        sendRoxieRequest(wsinfo.qsetname.get(), soapmsg, output, status, wsinfo.queryname);
        Owned<IWuWebView> web = createWuWebView(*wsinfo.wu, wsinfo.queryname.get(), getCFD(), true);
        if (!view)
            web->applyResultsXSLT(xsltfile.str(), output.str(), html);
        else
            web->renderResults(view, output.str(), html);
    }
    else
    {
        submitWsEclWorkunit(context, wsinfo, soapmsg.str(), html, 0, view, xsltfile.str());
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
        content.appendf("<message name=\"%sSoapIn\">", wsinfo->queryname.sget());
        content.appendf("<part name=\"parameters\" element=\"tns:%sRequest\"/>", wsinfo->queryname.sget());
        content.append("</message>");

        content.appendf("<message name=\"%sSoapOut\">", wsinfo->queryname.sget());
        content.appendf("<part name=\"parameters\" element=\"tns:%sResponse\"/>", wsinfo->queryname.sget());
        content.append("</message>");
    }

    return 0;
}

int CWsEclBinding::getWsdlPorts(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda)
{
    WsEclWuInfo *wsinfo = (WsEclWuInfo *) context.getBindingValue();
    if (wsinfo)
    {
        content.appendf("<portType name=\"%sServiceSoap\">", wsinfo->qsetname.sget());
        content.appendf("<operation name=\"%s\">", wsinfo->queryname.sget());
        content.appendf("<input message=\"tns:%sSoapIn\"/>", wsinfo->queryname.sget());
        content.appendf("<output message=\"tns:%sSoapOut\"/>", wsinfo->queryname.sget());
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
        content.appendf("<binding name=\"%sServiceSoap\" type=\"tns:%sServiceSoap\">", wsinfo->qsetname.sget(), wsinfo->qsetname.sget());
        content.append("<soap:binding transport=\"http://schemas.xmlsoap.org/soap/http\" style=\"document\"/>");

        content.appendf("<operation name=\"%s\">", wsinfo->queryname.sget());
        content.appendf("<soap:operation soapAction=\"/%s/%s?ver_=1.0\" style=\"document\"/>", wsinfo->qsetname.sget(), wsinfo->queryname.sget());
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
    EspHttpBinding::onGetWsdl(context, request, response, wsinfo.qsetname.sget(), wsinfo.queryname.sget());
    context.setBindingValue(NULL);
    return 0;
}

int CWsEclBinding::onGetXsd(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo)
{
    context.setBindingValue(&wsinfo);
    EspHttpBinding::onGetXsd(context, request, response, wsinfo.qsetname.sget(), wsinfo.queryname.sget());
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
                xsds_tree.setown(createPTreeFromXMLString(xsds.str()));
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
    WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());

    context->setBindingValue(&wsinfo);
    if (!stricmp(exampletype.str(), "request"))
        return onGetReqSampleXml(*context, request, response, qs.str(), qid.str());
    else if (!stricmp(exampletype.str(), "response"))
    {
        StringBuffer output;
        buildSampleResponseXml(output, *context, request, wsinfo);
        if (output.length())
            {
                response->setStatus("200 OK");
                response->setContent(output.str());
                response->setContentType(HTTP_TYPE_APPLICATION_XML);
                response->send();
            }
    }
    context->setBindingValue(NULL);
    return 0;
}

int CWsEclBinding::onGet(CHttpRequest* request, CHttpResponse* response)
{
    Owned<IMultiException> me = MakeMultiException("WsEcl");

    try
    {
        IEspContext *context = request->queryContext();
        IProperties *parms = request->queryParameters();

        if (!context->validateFeatureAccess(WSECL_ACCESS, SecAccess_Full, false))
            throw MakeStringException(-1, "WsEcl access permission denied.");

        const char *thepath = request->queryPath();

        StringBuffer serviceName;
        firstPathNode(thepath, serviceName);

        if (stricmp(serviceName.str(), "WsEcl"))
            return EspHttpBinding::onGet(request, response);

        StringBuffer methodName;
        nextPathNode(thepath, methodName);

        if (!stricmp(methodName.str(), "tabview"))
        {
            return getWsEcl2TabView(request, response, thepath);
        }
        else if (!stricmp(methodName.str(), "forms"))
        {
            return getWsEcl2Form(request, response, thepath);
        }
        else if (!stricmp(methodName.str(), "submit"))
        {
            StringBuffer wuid;
            StringBuffer qs;
            StringBuffer qid;

            splitLookupInfo(parms, thepath, wuid, qs, qid);

            StringBuffer format;
            nextPathNode(thepath, format);

            WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());
            return onSubmitQueryOutputXML(*context, request, response, wsinfo, format.str());
        }
        else if (!stricmp(methodName.str(), "xslt"))
        {
            StringBuffer wuid;
            StringBuffer qs;
            StringBuffer qid;

            splitLookupInfo(parms, thepath, wuid, qs, qid);
            WsEclWuInfo wsinfo(wuid.str(), qs.str(), qid.str(), context->queryUserId(), context->queryPassword());

            return onSubmitQueryOutputView(*context, request, response, wsinfo);
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
    
    response->handleExceptions(getXslProcessor(), me, "WsEcl", "", StringBuffer(getCFD()).append("./smc_xslt/exceptions.xslt").str());
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


void createPTreeFromJsonString(const char *json, bool caseInsensitive, StringBuffer &xml, const char *tail);

void CWsEclBinding::handleJSONPost(CHttpRequest *request, CHttpResponse *response)
{
    IEspContext *ctx = request->queryContext();
    IProperties *parms = request->queryParameters();
    StringBuffer jsonresp;

    try
    {
        if (!ctx->validateFeatureAccess(WSECL_ACCESS, SecAccess_Full, false))
            throw MakeStringException(-1, "WsEcl access permission denied.");

        const char *thepath = request->queryPath();

        StringBuffer serviceName;
        firstPathNode(thepath, serviceName);

        if (!strieq(serviceName.str(), "WsEcl"))
            EspHttpBinding::handleHttpPost(request, response);

        StringBuffer action;
        nextPathNode(thepath, action);

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

        WsEclWuInfo wsinfo(wuid.str(), queryset.str(), queryname.str(), ctx->queryUserId(), ctx->queryPassword());
        SCMStringBuffer clustertype;
        wsinfo.wu->getDebugValue("targetclustertype", clustertype);

        StringBuffer content(request->queryContent());
        StringBuffer status;
        if (strieq(clustertype.str(), "roxie"))
        {
            StringBuffer output;
            DBGLOG("json req: %s", content.str());
            sendRoxieRequest(wsinfo.qsetname.get(), content, jsonresp, status, wsinfo.queryname, "application/json");
            DBGLOG("json resp: %s", jsonresp.str());
        }
        else
        {
            StringBuffer soapfromjson;
            soapfromjson.append(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
                  " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">"
                    " <soap:Body>"
                );
            createPTreeFromJsonString(content.str(), false, soapfromjson, "Request");
            soapfromjson.append("</soap:Body></soap:Envelope>");
            DBGLOG("soap from json req: %s", soapfromjson.str());

            StringBuffer soapresp;
            unsigned xmlflags = WWV_ADD_SOAP | WWV_ADD_RESULTS_TAG | WWV_ADD_RESPONSE_TAG | WWV_INCL_NAMESPACES | WWV_INCL_GENERATED_NAMESPACES;
            if (ctx->queryRequestParameters()->hasProp("display"))
                xmlflags |= WWV_USE_DISPLAY_XSLT;
            if (streq(action.str(), "expanded"))
                xmlflags |= WWV_CDATA_SCHEMAS;
            else
                xmlflags |= WWV_OMIT_SCHEMAS;

            submitWsEclWorkunit(*ctx, wsinfo, soapfromjson.str(), soapresp, xmlflags);
            DBGLOG("HandleSoapRequest response: %s", soapresp.str());
            getWsEclJsonResponse(jsonresp, *ctx, request, soapresp.str(), wsinfo);
        }

    }
    catch (IException *e)
    {
        appendJSONException(jsonresp.set("{"), e);
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
    IProperties *parms = request->queryParameters();

    const char *thepath = request->queryPath();

    StringBuffer serviceName;
    firstPathNode(thepath, serviceName);

    if (!strieq(serviceName.str(), "WsEcl"))
        return CHttpSoapBinding::HandleSoapRequest(request, response);

    if(ctx->toBeAuthenticated()) //future support WsSecurity tags?
    {
        response->sendBasicChallenge(getChallengeRealm(), false);
        return 0;
    }

    if (!ctx->validateFeatureAccess(WSECL_ACCESS, SecAccess_Full, false))
        throw MakeStringException(-1, "WsEcl access permission denied.");

    StringBuffer action;
    nextPathNode(thepath, action);

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

    WsEclWuInfo wsinfo(wuid.str(), queryset.str(), queryname.str(), ctx->queryUserId(), ctx->queryPassword());

    StringBuffer content(request->queryContent());
    StringBuffer soapresp;
    StringBuffer status;

    SCMStringBuffer clustertype;
    wsinfo.wu->getDebugValue("targetclustertype", clustertype);

    unsigned xmlflags = WWV_ADD_SOAP | WWV_ADD_RESULTS_TAG | WWV_ADD_RESPONSE_TAG | WWV_INCL_NAMESPACES | WWV_INCL_GENERATED_NAMESPACES;
    if (ctx->queryRequestParameters()->hasProp("display"))
        xmlflags |= WWV_USE_DISPLAY_XSLT;
    if (streq(action.str(), "expanded"))
        xmlflags |= WWV_CDATA_SCHEMAS;
    else
        xmlflags |= WWV_OMIT_SCHEMAS;

    if (strieq(clustertype.str(), "roxie"))
    {
        StringBuffer content(request->queryContent());
        StringBuffer output;
        sendRoxieRequest(wsinfo.qsetname.get(), content, output, status, wsinfo.queryname);
        Owned<IWuWebView> web = createWuWebView(*wsinfo.wu, wsinfo.queryname.get(), getCFD(), true);
        if (web.get())
            web->expandResults(output.str(), soapresp, xmlflags);
    }
    else
        submitWsEclWorkunit(*ctx, wsinfo, content.str(), soapresp, xmlflags);

    DBGLOG("HandleSoapRequest response: %s", soapresp.str());

    response->setContent(soapresp.str());
    response->setContentType("text/xml");
    response->setStatus("200 OK");
    response->send();

    return 0;
}

