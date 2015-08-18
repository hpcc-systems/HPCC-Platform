#include "WizardBase.hpp"
#include "XMLTags.h"
#include "jstring.hpp"
#include "jptree.hpp"

CWizardBase::CWizardBase()
{
}

CWizardBase::~CWizardBase()
{
}

bool CWizardBase::generate(CEnvironmentConfiguration *pConfig)
{
    StringBuffer xpath;

    xpath.clear().appendf("<%s><%s></%s>", XML_HEADER, XML_TAG_ENVIRONMENT, XML_TAG_ENVIRONMENT);

/*
    xpath.clear().appendf("%s/%s/%s[%s='%s']/LocalEnvFile", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, m_service.str());

    const char* pConfFile = m_cfg->queryProp(xpath.str());
*/
    return true;
}
