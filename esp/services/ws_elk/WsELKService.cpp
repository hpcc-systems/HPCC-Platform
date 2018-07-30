#include "WsELKService.hpp"

Cws_elkEx::Cws_elkEx()
{
}

Cws_elkEx::~Cws_elkEx()
{
}

void Cws_elkEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if(cfg == nullptr)
        throw MakeStringException(-1, "Cannot initialize Cws_elkEx, cfg is NULL");

    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    m_serviceCfg.setown(cfg->getPropTree(xpath.str()));

#ifdef _DEBUG
    StringBuffer thexml;
    toXML(m_serviceCfg, thexml,0,0);
    DBGLOG("^^^^^^%s", thexml.str());
#endif
}

bool Cws_elkEx::onGetConfigDetails(IEspContext &context, IEspGetConfigDetailsRequest &req, IEspGetConfigDetailsResponse &resp)
{
    if (m_serviceCfg)
    {
        if (!m_serviceCfg->hasProp("ELKIntegration/Kibana/@integrateKibana"))
        {
            resp.setIntegrateKibana(false);
            return true;
        }

        StringBuffer kibanacfgentry;
        resp.setIntegrateKibana(m_serviceCfg->getPropBool("ELKIntegration/Kibana/@integrateKibana", false));
        m_serviceCfg->getProp("ELKIntegration/Kibana/@kibanaAddress", kibanacfgentry.clear());
        resp.setKibanaAddress(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/Kibana/@kibanaPort", kibanacfgentry.clear());
        resp.setKibanaPort(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/Kibana/@kibanaEntryPointURI", kibanacfgentry.clear());
        resp.setKibanaEntryPointURI(kibanacfgentry.str());
        resp.setReportElasticSearchHealth(m_serviceCfg->getPropBool("ELKIntegration/ElasticSearch/@reportElasticHealth", false));
        m_serviceCfg->getProp("ELKIntegration/ElasticSearch/@elasticSearchAddresses", kibanacfgentry.clear());
        resp.setElasticSearchAddresses(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/ElasticSearch/@elasticSearchPort", kibanacfgentry.clear());
        resp.setElasticSearchPort(kibanacfgentry.str());
        resp.setReportLogStashHealth(m_serviceCfg->getPropBool("ELKIntegration/LogStash/@reportLogStashHealth", false));
        m_serviceCfg->getProp("ELKIntegration/LogStash/@logStashAddresses", kibanacfgentry.clear());
        resp.setLogStashAddress(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/LogStash/@logStashPort", kibanacfgentry.clear());
        resp.setLogStashPort(kibanacfgentry.str());
        return true;
    }
    return false;
}
