/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "jliball.hpp"
#include "hqlwuerr.hpp"


class CompoundErrorReceiver : implements IErrorReceiver, public CInterface
{
public:
    CompoundErrorReceiver(IErrorReceiver * _primary, IErrorReceiver * _secondary) { primary.set(_primary); secondary.set(_secondary); }
    IMPLEMENT_IINTERFACE;

    virtual IError * mapError(IError * error)
    {
        Owned<IError> mappedError = primary->mapError(error);
        assertex(mappedError == error); // should not expect any mapping below a compound.
        return mappedError.getClear();
    }
    virtual void exportMappings(IWorkUnit * wu) const
    {
        // should not expect any mapping below a compound.
    }
    virtual void report(IError* err)
    {
        primary->report(err);
        secondary->report(err);
    }
    virtual size32_t errCount()    { return primary->errCount(); }
    virtual size32_t warnCount()       { return primary->warnCount(); }

private:
    Owned<IErrorReceiver> primary;
    Owned<IErrorReceiver> secondary;
};

extern HQL_API IErrorReceiver * createCompoundErrorReceiver(IErrorReceiver * primary, IErrorReceiver * secondary)
{
    return new CompoundErrorReceiver(primary, secondary);
}
