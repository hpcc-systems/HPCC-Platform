/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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
#pragma warning (disable : 4786)

#include "esdl_svc_engine.hpp"
#include "params2xml.hpp"

void CEsdlSvcEngine::init(const IPropertyTree *cfg, const char *process, const char *service)
{
    EsdlServiceImpl::init(cfg, process, service);

    m_service_ctx.setown( createPTree("Context", false) );
    ensurePTree(m_service_ctx, "Row");
}

CEsdlSvcEngine::~CEsdlSvcEngine()
{
}

CEsdlSvcEngineSoapBindingEx::CEsdlSvcEngineSoapBindingEx()
{
}

CEsdlSvcEngineSoapBindingEx::CEsdlSvcEngineSoapBindingEx(IPropertyTree* cfg, IPropertyTree *esdlArchive, const char *bindname, const char *procname) : EsdlBindingImpl(cfg, esdlArchive, bindname, procname)
{
}

IPropertyTree *createContextMethodConfig(IPropertyTree *methodConfig)
{
    const char *include = "*";
    if (methodConfig->hasProp("@contextInclude"))
        include = methodConfig->queryProp("@contextInclude");
    const char *remove = "Transforms|xsdl:CustomRequestTransform";
    if (methodConfig->hasProp("@contextRemove"))
        remove = methodConfig->queryProp("@contextRemove");
    const char *removeAttrs = "@contextInclude|@contextRemove|@contextAttRemove";
    if (methodConfig->hasProp("@contextAttRemove"))
        removeAttrs = methodConfig->queryProp("@contextAttRemove");

    Owned<IPropertyTree> contextConfig;
    if (include && !streq(include, "*"))
    {
        contextConfig.setown(createPTree("Method", ipt_ordered));
        Owned<IAttributeIterator> aiter = methodConfig->getAttributes(); //for now include all attributes
        ForEach (*aiter)
            contextConfig->addProp(aiter->queryName(), aiter->queryValue());

        if (*include) //if *include==0 we should have an empty tree
        {
            StringArray xpaths;
            xpaths.appendListUniq(include, "|", true); //don't supported quoted | for now
            ForEachItemIn(pos, xpaths)
            {
                Owned<IPropertyTreeIterator> toInclude = methodConfig->getElements(xpaths.item(pos));
                ForEach(*toInclude)
                    contextConfig->addPropTree(toInclude->query().queryName(), LINK(&toInclude->query()));
            }
        }
    }
    if (remove && *remove)
    {
        if (!contextConfig)
            contextConfig.setown(createPTreeFromIPT(methodConfig, ipt_ordered));
        if (contextConfig->hasChildren())
        {
            StringArray xpaths;
            xpaths.appendListUniq(remove, "|", true); //don't supported quoted | for now
            ForEachItemIn(pos, xpaths)
            {
                Owned<IPropertyTreeIterator> toRemove = contextConfig->getElements(xpaths.item(pos));
                ForEach(*toRemove)
                    contextConfig->removeTree(&toRemove->query());
            }
        }
    }

    if (removeAttrs && *removeAttrs)
    {
        if (!contextConfig)
            contextConfig.setown(createPTreeFromIPT(methodConfig, ipt_ordered));
        StringArray xpaths;
        StringBuffer temp;
        if (streq(removeAttrs, "*"))
        {
            Owned<IAttributeIterator> attrs = contextConfig->getAttributes();
            ForEach(*attrs)
            {
                if (!temp.isEmpty())
                    temp.append('|');
                temp.append(attrs->queryName());
            }
            removeAttrs=temp.str();
        }

        xpaths.appendListUniq(removeAttrs, "|", true); //don't support quoted | for now

        ForEachItemIn(pos, xpaths)
            contextConfig->removeProp(xpaths.item(pos));
    }

    if (contextConfig)
        return contextConfig.getClear();
    return LINK(methodConfig); //no copy, nothing changed
}

bool skipContextConfig(IPropertyTree *cfg)
{
    const char *elRemove = cfg->queryProp("@contextRemove");
    if (elRemove && streq(elRemove, ".")) //remove entire node
        return true;

    const char *elInclude = cfg->queryProp("@contextInclude");
    const char *attRemove = cfg->queryProp("@contextAttRemove");

    bool noElIncluded = elInclude && !*elInclude; //empty xpath
    bool allElRemoved = (elRemove && streq(elRemove, "*"));  //remove all child elements
    bool allAttRemoved = (attRemove && streq(attRemove, "*")); //remove top level attributes (affects root node only)

    //note that @contextRemove="." is equivalent to (@contextRemove="*" and @contextAttRemove="*") because the empty root tag is removed
    return (allAttRemoved && (noElIncluded || allElRemoved));
}

IPropertyTree *CEsdlSvcEngine::createTargetContext(IEspContext &context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *req_pt)
{
    Owned<IPropertyTree> localCtx(createPTreeFromIPT(m_service_ctx, ipt_none));
    ensurePTree(localCtx, "Row/Common");
    localCtx->setProp("Row/Common/TransactionId", context.queryTransactionID());
    ensurePTree(localCtx, "Row/Common/ESP");
    localCtx->setProp("Row/Common/ESP/ServiceName", context.queryServiceName(""));
    //removing this entry since the Row/Common/ESP/Config/Method tree should have an attribute @name
    //localCtx->setProp("Row/Common/ESP/MethodName", mthdef.queryMethodName());
    if (!skipContextConfig(tgtcfg))
    {
        Owned<IPropertyTree> config = createContextMethodConfig(tgtcfg);
        if (config)
        {
            Owned<IAttributeIterator> ai = config->getAttributes();
            if (config->hasChildren() || ai->count()>0)
            {
                ensurePTree(localCtx, "Row/Common/ESP/Config");
                localCtx->addPropTree("Row/Common/ESP/Config/Method", config.getClear());
            }
        }
    }
    return localCtx.getLink();
}

void CEsdlSvcEngine::generateTransactionId(IEspContext & context, StringBuffer & trxid)
{
    //creationtime_threadid_RANDOMNUM for now.
    trxid.appendf("%u_%u_%u",context.queryCreationTime(), ((unsigned) (memsize_t) GetCurrentThreadId()), getRandom());
}

void CEsdlSvcEngine::esdl_log(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *tgtcfg, IPropertyTree *tgtctx, IPropertyTree *req_pt, const char *rawresp, const char *logdata, unsigned int timetaken)
{
}
