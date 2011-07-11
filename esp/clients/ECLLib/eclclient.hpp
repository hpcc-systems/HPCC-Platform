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

#ifndef _ECLCLIENT_HPP__
#define _ECLCLIENT_HPP__

#include "ecllib.hpp"

class CClientInstantEcl : public CInterface,
   implements IClientInstantEcl
{
private:
    char m_url[256];
    long m_reqId;
public:
    IMPLEMENT_IINTERFACE;

    CClientInstantEcl();
    virtual ~CClientInstantEcl();

    virtual void addServiceUrl(const char * url);
    virtual void removeServiceUrl(const char * url);
    virtual IClientInstantEclRequest * createRequest();
    virtual IClientInstantEclResp* runEcl(IClientInstantEclRequest * request);
    virtual void runEclAsync(IClientInstantEclRequest * request, IClientInstantEclEvents &events);

};



#endif
