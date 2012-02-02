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

#ifndef _ESPWIZ_EclDirect_HPP__
#define _ESPWIZ_EclDirect_HPP__

#include "ecldirect_esp.ipp"

class CEclDirectEx : public CEclDirect
{
private:
    StringBuffer m_clustername;
    StringBuffer m_eclserver;
    int m_def_timeout;
    bool m_deleteworkunits;

public:
   IMPLEMENT_IINTERFACE;

    CEclDirectEx() : m_def_timeout(0){}

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    bool onRunEcl(IEspContext &context, IEspRunEclRequest &req, IEspRunEclResponse &resp);
    bool onRunEclEx(IEspContext &context, IEspRunEclExRequest &req, IEspRunEclExResponse &resp);
};

#endif //_ESPWIZ_EclDirect_HPP__

