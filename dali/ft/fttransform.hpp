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

#ifndef FTTRANSFORM_HPP
#define FTTRANSFORM_HPP
#include "rmtfile.hpp"

#include "ftbase.hpp"

interface ITransformer : public IInterface
{
public:
    virtual void beginTransform(IFileIOStream * out) = 0;
    virtual void endTransform(IFileIOStream * out) = 0;
    virtual size32_t getBlock(IFileIOStream * out) = 0;
    virtual bool getInputCRC(crc32_t & value) = 0;
    virtual void setInputCRC(crc32_t value) = 0;
    virtual bool setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length, bool compressedInput, const char *decryptKey) = 0;
    virtual offset_t tell() = 0;
};


#endif
