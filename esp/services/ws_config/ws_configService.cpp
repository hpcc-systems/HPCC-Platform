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

#pragma warning (disable : 4786)

#include "ws_configService.hpp"

static const char* FEATURE_URL = "ConfigAccess";

void Cws_configEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
}

Cws_configEx::~Cws_configEx()
{
}

bool Cws_configEx::onGetConfigAccess(IEspContext &context, IEspConfigAccessRequest& req, IEspConfigAccessResponse& resp)
{
    SecAccessFlags access = SecAccess_None;
    context.authorizeFeature(FEATURE_URL, access);
    resp.setAccessLevel( access );
    return true;
}
