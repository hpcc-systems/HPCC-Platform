/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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
