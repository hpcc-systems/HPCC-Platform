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

#ifndef __ESP_ERRORS_HPP__
#define __ESP_ERRORS_HPP__


namespace EspCoreErrors
{
    const unsigned int Base = 1000;
}

namespace WsGatewayErrors
{
    const unsigned int Base = EspCoreErrors::Base+200;   //1200 -- pls don't change this....
    const unsigned int MissingConfiguration = Base+0;
    const unsigned int MissingUserName      = Base+1;
    const unsigned int MissingPassword      = Base+2;
    const unsigned int MissingOptions       = Base+3;
    const unsigned int MissingUrl           = Base+4;
    const unsigned int InvalidUrl           = Base+5;
    const unsigned int NoResponse           = Base+6;
}

namespace WsAccurintErrors
{
    const unsigned int Base = EspCoreErrors::Base+1005;   //2005 -- pls don't change this....the web is coded to expect the following codes
    const unsigned int RealTimeInvalidUse       = Base+0; //"Access to realtime data is not allowed for your intended use."
    const unsigned int RealTimeInvalidState     = Base+1; //"Access to realtime search is not allowed under the DPPA guidelines for your intended use for that state."
    const unsigned int RealTimeMissingStateZip  = Base+2; //"A valid state or zip must be specified for realtime data."
    const unsigned int RealTimeMissingName      = Base+3; //"A person or company name must be specified for realtime data."
    const unsigned int RealTimeMissingAddress   = Base+4; //"Street address with City+State or Zip must be specified for realtime data."
}


#endif //__ESP_ERRORS_HPP__
