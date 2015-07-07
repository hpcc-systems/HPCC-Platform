#ifndef _ENVIRONMENT_CONFIGURATON_HPP_
#define _ENVIRONMENT_CONFIGURATON_HPP_

#include "jptree.hpp"
#include "ConfigFileComponentUtils.hpp"


class CEnvironmentConfiguration : public CConfigFileComponentUtils
{
public:

    IMPLEMENT_IINTERFACE

    enum CEF_ERROR_CODES{ CF_NO_ERROR = 0,
                          CF_UNKNOWN_COMPONENT,
                          CF_UNKNOWN_ESP_COMPONENT,
                          CF_COMPONENT_INSTANCE_NOT_FOUND,
                          CF_OTHER = 0xFF };

    static CEnvironmentConfiguration* getInstance();

    virtual ~CEnvironmentConfiguration();

    enum CEF_ERROR_CODES generateBaseEnvironmentConfiguration();
    enum CEF_ERROR_CODES addComponent(const char* pCompType);
    enum CEF_ERROR_CODES removeComponent(const char* pCompType, const char* pCompName);
    enum CEF_ERROR_CODES addESPService(const char* espServiceType);



protected:

    CEnvironmentConfiguration();
    Owned<IPropertyTree> m_pEnv;


private:


};

#endif // _ENVIRONMENT_CONFIGURATON_HPP_
