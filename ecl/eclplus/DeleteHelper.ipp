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
#ifndef DELETEHELPER_HPP
#define DELETEHELPER_HPP

#include "jlib.hpp"
#include "eclplus.hpp"
#include "formattypes.ipp"
#include "ws_workunits.hpp"

class DeleteHelper : implements IEclPlusHelper
{
public:
    IMPLEMENT_IINTERFACE;

    DeleteHelper(IProperties * globals, IFormatType * format);
    virtual ~DeleteHelper();

    virtual bool doit(FILE * fp);

protected:
    Owned<IProperties> globals;
    Owned<IFormatType> format;
    Owned<IClientWsWorkunits> wuclient;
};

class AbortHelper : implements IEclPlusHelper
{
public:
    IMPLEMENT_IINTERFACE;

    AbortHelper(IProperties * globals, IFormatType * format);
    virtual bool doit(FILE * fp);

protected:
    Owned<IProperties> globals;
    Owned<IFormatType> format;
    Owned<IClientWsWorkunits> wuclient;
};
#endif // DELETEHELPER_HPP
