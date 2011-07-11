/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
