#include "jliball.hpp"
#include "ws_ecl_wuinfo.hpp"
#include "fileview.hpp"

WsEclWuInfo::WsEclWuInfo(const char *wuid_, const char *qset, const char *qname, const char *user, const char *pw) :
    wuid(wuid_), username(user), password(pw), qsetname(qset), queryname(qname)
{
}

const char *WsEclWuInfo::ensureWuid()
{
    if (wuid.length())
        return wuid.get();
    if (qsetname.length() && queryname.length())
    {
        Owned<IPropertyTree> qstree = getQueryRegistry(qsetname, true);
        if (!qstree)
            throw MakeStringException(-1, "QuerySet %s not found", qsetname.get());

        Owned<IPropertyTree> query = resolveQueryAlias(qstree, queryname);
        if (!query)
            throw MakeStringException(-1, "Query %s/%s not found", qsetname.get(), queryname.get());
        if (query->getPropBool("@suspended"))
            throw MakeStringException(-1, "Query %s/%s is currently suspended", qsetname.get(), queryname.get());

        wuid.set(query->queryProp("@wuid"));
    }
    if (!wuid.length())
        throw MakeStringException(-1, "Workunit not specified");
    return wuid.get();
}

IConstWorkUnit *WsEclWuInfo::ensureWorkUnit()
{
    if (!wuid.length())
        ensureWuid();
    if (wu)
        return wu;
    Owned<IWorkUnitFactory> wf = getWorkUnitFactory();
    wu.setown(wf->openWorkUnit(wuid.str()));
    if (!wu)
        throw MakeStringException(-1, "Could not open workunit: %s", wuid.str());
    if (isLibrary(wu))
        throw MakeStringException(-1, "%s/%s %s is a library", qsetname.str(), queryname.str(), wuid.str());
    return wu;
}

void appendVariableParmInfo(IArrayOf<IPropertyTree> &parts, IResultSetFactory *resultSetFactory, IConstWUResult &var, unsigned hashWebserviceSeq=0)
{
    Owned<IResultSetMetaData> meta = resultSetFactory->createResultSetMeta(&var);
    StringAttr noinput;
    if (var.getResultFieldOpt("noinput", StringAttrAdaptor(noinput)).length() && strToBool(noinput.length(), noinput.get()))  //developer specified not to show field on form
        return;

    SCMStringBuffer varname;
    var.getResultName(varname);
    int seq = var.getResultSequence();

    WUResultFormat fmt = var.getResultFormat();

    SCMStringBuffer eclschema;
    var.getResultEclSchema(eclschema);

    StringBuffer width, height, fieldSeq, isPassword, select;
    var.getResultFieldOpt("fieldwidth", StringBufferAdaptor(width));
    var.getResultFieldOpt("fieldheight", StringBufferAdaptor(height));
    var.getResultFieldOpt("password", StringBufferAdaptor(isPassword));
    var.getResultFieldOpt("select", StringBufferAdaptor(select));
    if (hashWebserviceSeq)
        fieldSeq.append(hashWebserviceSeq);
    else
        var.getResultFieldOpt("sequence", StringBufferAdaptor(fieldSeq));

    SCMStringBuffer s;
    Owned<IPropertyTree> part = createPTree("part");
    if (!var.isResultScalar())
    {
        meta->getXmlSchema(s, false);
        part->setProp("@name", varname.str());
        part->setProp("@type", "tns:XmlDataset");
        if (fieldSeq.length())
            part->setProp("@sequence", fieldSeq);
    }
    else
    {
        meta->getColumnEclType(s, 0);
        DisplayType dt = meta->getColumnDisplayType(0);
        StringAttr ptype;
        switch (dt)
        {
        case TypeBoolean:
            ptype.set("xsd:boolean");
            break;
        case TypeInteger:
            ptype.set("xsd:integer");
            break;
        case TypeUnsignedInteger:
            ptype.set("xsd:integer");
            break;
        case TypeReal:
            ptype.set("xsd:real");
            break;
        case TypeSet:
            ptype.set("tns:EspStringArray");
            break;
        case TypeDataset:
        case TypeData:
            ptype.set("tns:XmlDataSet");
            break;
        case TypeUnicode:
        case TypeString:
            ptype.set("xsd:string");
            break;
        case TypeUnknown:
        case TypeBeginIfBlock:
        case TypeEndIfBlock:
        case TypeBeginRecord:
        default:
            ptype.set("xsd:string");
            break;
        }
        part->setProp("@name", varname.str());
        part->setProp("@type", ptype.str());
        if (width.length())
            part->setProp("@width", width);
        if (height.length())
            part->setProp("@height", height);
        if (fieldSeq.length())
            part->setProp("@sequence", fieldSeq);
        if (isPassword.length())
            part->setProp("@password", isPassword);
        if (select.length())
        {
            StringArray optionList;
            optionList.appendList(select, ",");
            IPropertyTree *selectTree = ensurePTree(part, "select");
            ForEachItemIn(i, optionList)
            {
                const char *value = optionList.item(i);
                bool selected = '*'==*value;
                if (selected)
                    value++;
                StringAttr name;
                const char *eq = strchr(value, '=');
                if (!eq)
                    name.set(value);
                else
                {
                    name.set(value, eq-value);
                    value = eq + 1;
                }
                Owned<IPropertyTree> optionTree = createPTree();
                optionTree->setProp("@name", name);
                optionTree->setProp("@value", value);
                if (selected)
                    optionTree->setPropBool("@selected", true);
                selectTree->addPropTree("option", optionTree.getClear());
            }
        }
    }
    parts.append(*part.getClear());
}

int orderMatchingSequence(IPropertyTree * left, IPropertyTree * right)
{
    if (!right->hasProp("@name"))
        return -1;
    if (!left->hasProp("@name"))
        return 1;
    return stricmp(left->queryProp("@name"), right->queryProp("@name"));
}

int orderParts(IInterface * const * pLeft, IInterface * const * pRight)
{
    IPropertyTree * left = (IPropertyTree *)*pLeft;
    IPropertyTree * right = (IPropertyTree *)*pRight;
    bool hasLeftSeq = left->hasProp("@sequence");
    bool hasRightSeq = right->hasProp("@sequence");
    if (hasLeftSeq && hasRightSeq)
    {
        int rightSeq = right->getPropInt("@sequence");
        int leftSeq = left->getPropInt("@sequence");
        if (rightSeq == leftSeq)
            return orderMatchingSequence(left, right);  //fields with same sequence alphabetical within sequence
        return leftSeq - rightSeq;
    }
    if (hasRightSeq)
        return 1;
    if (hasLeftSeq)
        return -1;
    return orderMatchingSequence(left, right);  //fields without sequence alphabetical AFTER sequenced fields
}

bool WsEclWuInfo::getWsResource(const char *name, StringBuffer &out)
{
    if (strieq(name, "SOAP"))
    {
        out.appendf("<message name=\"%s\">", queryname.str());
        Owned<IResultSetFactory> resultSetFactory = getResultSetFactory(username, password);
        Owned<IConstWUWebServicesInfo> wsinfo = ensureWorkUnit()->getWebServicesInfo();
        StringArray fields;
        if (wsinfo)
        {
            SCMStringBuffer fieldList;
            wsinfo->getText("fields", fieldList);
            if (fieldList.length())
                fields.appendListUniq(fieldList.str(), ",");
        }
        IArrayOf<IPropertyTree> parts;
        if (fields.length())
        {
            ForEachItemIn(i, fields)
            {
                Owned<IConstWUResult> var = wu->getVariableByName(fields.item(i));
                if (var)
                    appendVariableParmInfo(parts, resultSetFactory, *var, i+1);
            }
        }
        else
        {
            Owned<IConstWUResultIterator> vars = &ensureWorkUnit()->getVariables();
            ForEach(*vars)
                appendVariableParmInfo(parts, resultSetFactory, vars->query());
        }
        parts.sort(orderParts);
        ForEachItemIn(i, parts)
            toXML(&parts.item(i), out);
        out.append("</message>");
    }

    return true;
}

IPropertyTree *WsEclWuInfo::queryParamInfo()
{
    if (!paraminfo)
    {
        StringBuffer xml;
        if (getWsResource("SOAP", xml))
            paraminfo.setown(createPTreeFromXMLString(xml.str()));
    }
    return paraminfo.get();
}


void WsEclWuInfo::addOutputSchemas(StringBuffer &schemas, IConstWUResultIterator *results, const char *tag)
{
    ForEach(*results)
    {
        StringBuffer s;
        getSchemaFromResult(s, results->query());
        SCMStringBuffer resultName;
        results->query().getResultName(resultName);
        StringBuffer sname =resultName.s.str();
        sname.replace(' ', '_');
        int seq = results->query().getResultSequence();
        schemas.appendf("<%s sequence=\"%d\" name=\"%s\" sname=\"%s\">%s</%s>", tag, seq, resultName.str(), sname.str(), s.str(), tag);
    }
}

void WsEclWuInfo::addInputSchemas(StringBuffer &schemas, IConstWUResultIterator *results, const char *tag)
{
    ForEach(*results)
    {
        if (!results->query().isResultScalar())
        {
            StringBuffer s;
            getSchemaFromResult(s, results->query());
            SCMStringBuffer resultName;
            results->query().getResultName(resultName);
            StringBuffer sname =resultName.s.str();
            sname.replace(' ', '_');

            int seq = results->query().getResultSequence();
            schemas.appendf("<%s sequence=\"%d\" name=\"%s\" sname=\"%s\">%s</%s>", tag, seq, resultName.str(), sname.str(), s.str(), tag);
        }
    }
}

void WsEclWuInfo::getSchemaFromResult(StringBuffer &schema, IConstWUResult &res)
{
//  if (!res.isResultScalar())
//  {
        StringBufferAdaptor s(schema);
        
        Owned<IResultSetFactory> resultSetFactory(getResultSetFactory(username, password));
        Owned<IResultSetMetaData> meta = resultSetFactory->createResultSetMeta(&res);
        meta->getXmlXPathSchema(s, true);
        const char *finger=schema.str();
        while (finger && strncmp(finger, "<xs:schema", 10))
            finger++;
//  }
}

void WsEclWuInfo::getInputSchema(StringBuffer &schema, const char *name)
{
    Owned<IConstWUResult> res =  ensureWorkUnit()->getResultByName(name);
    getSchemaFromResult(schema, *res);
}

void WsEclWuInfo::getOutputSchema(StringBuffer &schema, const char *name)
{
    Owned<IConstWUResult> res =  ensureWorkUnit()->getResultByName(name);
    getSchemaFromResult(schema, *res);
}


void WsEclWuInfo::updateSchemaCache()
{
    if (!schemacache.length())
    {
        schemacache.append("<SCHEMA>");

        Owned<IConstWUResultIterator> inputs = &ensureWorkUnit()->getVariables();
        addInputSchemas(schemacache, inputs, "Input");

        Owned<IConstWUResultIterator> results = &wu->getResults();
        addOutputSchemas(schemacache, results, "Result");
        
        schemacache.append("</SCHEMA>");
    }
}

void WsEclWuInfo::getSchemas(StringBuffer &schemas)
{
    updateSchemaCache();
    schemas.append(schemacache);
}

IPropertyTreeIterator *WsEclWuInfo::getInputSchemas()
{
    if (!xsds)
    {
        updateSchemaCache();
        if (schemacache.length())   
            xsds.setown(createPTreeFromXMLString(schemacache.str()));
    }

    return (xsds) ? xsds->getElements("Input") : NULL;
}

IPropertyTreeIterator *WsEclWuInfo::getResultSchemas()
{
    if (!xsds)
    {
        updateSchemaCache();
        if (schemacache.length())   
            xsds.setown(createPTreeFromXMLString(schemacache.str()));
    }

    return (xsds) ? xsds->getElements("Result") : NULL;
}
