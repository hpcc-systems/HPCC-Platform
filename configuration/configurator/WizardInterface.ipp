#ifndef _WIZARD_INTERFACE_IPP_
#define _WIZARD_INTERFACE_IPP_

#include "jiface.hpp"

class CEnvironmentConfiguration;

interface IWizardInterface : public CInterface
{
public:

    virtual bool generate(CEnvironmentConfiguration *pConfig) = 0;

};

#endif // _WIZARD_INTERFACE_IPP_
