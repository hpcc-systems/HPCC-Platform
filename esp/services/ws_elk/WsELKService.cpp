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

bool Cws_elkEx::ongetConfigDetails(IEspContext &context, IEspGetConfigDetailsRequest &req, IEspGetConfigDetailsResponse &resp)
{
    if (m_serviceCfg)
    {
        StringBuffer kibanacfgentry;
        m_serviceCfg->getProp("ELKIntegration/Kibana/@integrateKibana", kibanacfgentry);
        resp.setIntegrateKibana(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/Kibana/@kibanaAddress", kibanacfgentry.clear());
        resp.setKibanaAddress(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/Kibana/@kibanaPort", kibanacfgentry.clear());
        resp.setKibanaPort(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/Kibana/@kibanaEntryPointURI", kibanacfgentry.clear());
        resp.setKibanaEntryPointURI(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/ElasticSearch/@reportElasticHealth", kibanacfgentry.clear());
        resp.setReportElasticSearchHealth(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/ElasticSearch/@elasticSearchAdresses", kibanacfgentry.clear());
        resp.setElasticSearchAdresses(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/ElasticSearch/@elasticSearchPort", kibanacfgentry.clear());
        resp.setElasticSearchPort(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/LogStash/@reportLogStashHealth", kibanacfgentry.clear());
        resp.setReportLogStashHealth(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/LogStash/@logStashAdresses", kibanacfgentry.clear());
        resp.setLogStashAddress(kibanacfgentry.str());
        m_serviceCfg->getProp("ELKIntegration/LogStash/@logStashPort", kibanacfgentry.clear());
        resp.setLogStashPort(kibanacfgentry.str());
        return true;
    }
    return false;
}
