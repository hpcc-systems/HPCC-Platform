#ifndef _WIZARD_BASE_HPP
#define _WIZARD_BASE_HPP

#include "WizardInterface.ipp"

class CWizardBase : public IWizardInterface
{
public:

    IMPLEMENT_IINTERFACE

    CWizardBase();
    virtual ~CWizardBase();

    virtual bool generate(CEnvironmentConfiguration *pConfig);

protected:

private:

};

#endif // _WIZARD_BASE_HPP
