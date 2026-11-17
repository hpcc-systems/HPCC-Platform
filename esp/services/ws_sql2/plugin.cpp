#include "http/platform/httpprot.hpp"
#include "service.hpp"
#include "espplugin.hpp"

extern "C"
{
    ESP_FACTORY IEspService* esp_service_factory(const char* name, const char* type, IPropertyTree* cfg, const char* process)
    {
        if (strieq(type, "ws_sql2"))
        {
            Owned<CWsSQL2Ex> service = new CWsSQL2Ex;
            service->init(cfg, process, name);
            return service.getClear();
        }
        return nullptr;
    }

    ESP_FACTORY IEspRpcBinding* esp_binding_factory(const char* name, const char* type, IPropertyTree* cfg, const char* process)
    {
        if (strieq(type, "ws_sql2SoapBinding") || strieq(type, "ws_sql2_http"))
            return new CWsSQL2SoapBindingEx(cfg, name, process);
        return nullptr;
    }

    ESP_FACTORY IEspProtocol* esp_protocol_factory(const char* name, const char* type, IPropertyTree* cfg, const char* process)
    {
        return http_protocol_factory(name, type, cfg, process);
    }
}