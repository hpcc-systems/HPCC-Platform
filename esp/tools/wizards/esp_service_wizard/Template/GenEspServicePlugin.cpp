#pragma warning (disable : 4786)

#include "$$root$$_esp.ipp"

//ESP Bindings
#include "http/platform/httpprot.hpp"

//ESP Service
#include "$$root$$Service.hpp"

#include "espplugin.hpp"

extern "C"
{

//when we aren't loading dynamically
// Change the function names when we stick with dynamic loading.
ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "$$ESP_SERVICE$$")==0)
   {
      C$$ESP_SERVICE$$Ex* service = new C$$ESP_SERVICE$$Ex;
        service->init(cfg, process, name);
      return service;
   }
    else
    {
        throw MakeStringException(-1, "Unknown service type %s", type);
    }
   return NULL;
}
 
   

ESP_FACTORY IEspRpcBinding * esp_binding_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "$$ESP_SERVICE$$SoapBinding")==0)
   {
        C$$ESP_SERVICE$$SoapBinding* binding = new C$$ESP_SERVICE$$SoapBinding(cfg, name, process);
      return binding;
   }
    else
    {
        throw MakeStringException(-1, "Unknown binding type %s", type);
    }

   return NULL;
}



ESP_FACTORY IEspProtocol * esp_protocol_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
    if (strcmp(type, "http_protocol")==0)
    {
        return new CHttpProtocol;
    }
    else if(strcmp(type, "secure_http_protocol") == 0)
    {
        IPropertyTree *sslSettings;
        sslSettings = cfg->getPropTree(StringBuffer("Software/EspProcess[@name=\"").append(process).append("\"]").append("/EspProtocol[@name=\"").append(name).append("\"]").str());
        if(sslSettings != NULL)
        {
            return new CSecureHttpProtocol(sslSettings);
        }
        else
        {
            throw MakeStringException(-1, "can't find ssl settings in the config file");
        }
    }
    else
    {
        throw MakeStringException(-1, "Unknown protocol %s", name);
    }

    return NULL;
}

};
