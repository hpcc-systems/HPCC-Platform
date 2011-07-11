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

#ifndef THORHELPER_HPP
#define THORHELPER_HPP

#include "jiface.hpp"
#include "jptree.hpp"

#ifdef _WIN32
 #ifdef THORHELPER_EXPORTS
  #define THORHELPER_API __declspec(dllexport)
 #else
  #define THORHELPER_API __declspec(dllimport)
 #endif
#else
 #define THORHELPER_API
#endif

interface IXmlToRawTransformer : extends IInterface
{
    virtual IDataVal & transform(IDataVal & result, size32_t len, const void * text, bool isDataset) = 0;
    virtual IDataVal & transformTree(IDataVal & result, IPropertyTree &tree, bool isDataset) = 0;
};

interface ICsvToRawTransformer : extends IInterface
{
    virtual IDataVal & transform(IDataVal & result, size32_t len, const void * text, bool isDataset) = 0;
};

enum ThorReplyCodes { DAMP_THOR_ACK, DAMP_THOR_REPLY_GOOD, DAMP_THOR_REPLY_ERROR, DAMP_THOR_REPLY_ABORT, DAMP_THOR_REPLY_PAUSED };

#endif // THORHELPER_HPP
