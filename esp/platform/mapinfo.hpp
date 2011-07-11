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

#ifndef __MAPINFO_HPP__
#define __MAPINFO_HPP__

#include "jiface.hpp"

interface IMapInfo : public IInterface
{
    virtual double getMinVersion(const char* structName, const char* field) = 0;
    virtual double getMaxVersion(const char* structName, const char* field) = 0;
    virtual double getDeprVersion(const char* structName, const char* field) = 0;
    // support only one optional now
    virtual const char* getOptional(const char* structName, const char* field) = 0;

    virtual void addMaxVersion(const char* structName, const char* field, double version) = 0;
    virtual void addMinVersion(const char* structName, const char* field, double version) = 0;
    virtual void addDeprVersion(const char* structName, const char* field, double version) = 0;
    virtual void addOptional(const char* structName, const char* field, const char* option) = 0;

    // for debug
    virtual void toString(class StringBuffer& s) = 0;
};

IMapInfo* createMapInfo();
IMapInfo* createNullMapInfo();

#endif

