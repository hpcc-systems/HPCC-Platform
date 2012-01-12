#include "jliball.hpp"
#include "ws_ecl_wuinfo.hpp"
#include "fileview.hpp"

WsEclWuInfo::WsEclWuInfo(const char *wuid_, const char *qset, const char *qname, const char *user, const char *pw) :
    wuid(wuid_), qsetname(qset), queryname(qname), username(user), password(pw)
{
    Owned<IWorkUnitFactory> wf = getWorkUnitFactory();
    if (!wuid.length() && qsetname.length() && queryname.length())
    {
        Owned<IPropertyTree> qstree = getQueryRegistry(qsetname.sget(), true);
        if (qstree)
        {
            IPropertyTree *query = NULL;
            VStringBuffer xpath("Alias[@name=\"%s\"]", queryname.sget());
            IPropertyTree *alias = qstree->queryPropTree(xpath.str());
            if (alias)
            {
                const char *quid = alias->queryProp("@id");
                if (!quid)
                    throw MakeStringException(-1, "Alias %s/%s has no Query defined", qsetname.sget(), queryname.sget());
                xpath.clear().appendf("Query[@id='%s']", quid);
                query = qstree->queryPropTree(xpath.str());
                if (!query)
                    throw MakeStringException(-1, "Alias %s/%s refers to a non existing query %s", qsetname.sget(), queryname.sget(), quid);
            }
            else
            {
                xpath.clear().appendf("Query[@id=\"%s\"]", queryname.sget());
                query = qstree->queryPropTree(xpath.str());
            }
            if (query)
            {
                if (query->getPropBool("@suspended"))
                    throw MakeStringException(-1, "Query %s/%s is currently suspended", qsetname.sget(), queryname.sget());

                wuid.set(query->queryProp("@wuid"));
            }
            else
                throw MakeStringException(-1, "Query %s/%s not found", qsetname.sget(), queryname.sget());
        }
        else
            throw MakeStringException(-1, "QuerySet %s not found", qsetname.sget());
    }
        
    if (wuid.length())
    {
        wu.setown(wf->openWorkUnit(wuid.sget(), false));
        if (!wu)
            throw MakeStringException(-1, "Could not open workunit: %s", wuid.sget());
    }
    else
        throw MakeStringException(-1, "Workunit not specified");
}

bool WsEclWuInfo::getWsResource(const char *name, StringBuffer &out)
{
    if (strieq(name, "SOAP"))
    {
        out.appendf("<message name=\"%s\">", queryname.sget());
        IConstWUResultIterator &vars = wu->getVariables();
        Owned<IResultSetFactory> resultSetFactory(getResultSetFactory(username, password));
        ForEach(vars)
        {
            IConstWUResult &var = vars.query();
            SCMStringBuffer varname;
            var.getResultName(varname);
            int seq = var.getResultSequence();

            WUResultFormat fmt = var.getResultFormat();

            SCMStringBuffer eclschema;
            var.getResultEclSchema(eclschema);

            SCMStringBuffer s;
            Owned<IResultSetMetaData> meta = resultSetFactory->createResultSetMeta(&var);

            if (!var.isResultScalar())
            {
                meta->getXmlSchema(s, false);
                out.appendf("<part name=\"%s\" type=\"tns:XmlDataSet\" />", varname.str());
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

                out.appendf("<part name=\"%s\" type=\"%s\" />", varname.str(), ptype.sget());
            }

        }
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
        meta->getXmlSchema(s, true);
        const char *finger=schema.str();
        while (finger && strncmp(finger, "<xs:schema", 10))
            finger++;
//  }
}

void WsEclWuInfo::getInputSchema(StringBuffer &schema, const char *name)
{
    Owned<IConstWUResult> res =  wu->getResultByName(name);
    getSchemaFromResult(schema, *res);
}

void WsEclWuInfo::getOutputSchema(StringBuffer &schema, const char *name)
{
    Owned<IConstWUResult> res =  wu->getResultByName(name);
    getSchemaFromResult(schema, *res);
}


void WsEclWuInfo::updateSchemaCache()
{
    if (!schemacache.length())
    {
        schemacache.append("<SCHEMA>");

        Owned<IConstWUResultIterator> inputs = &wu->getVariables();
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
