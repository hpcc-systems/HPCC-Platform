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

#ifndef XMLERROR_HPP
#define XMLERROR_HPP

#include "jexcept.hpp"

/* Errors can occupy range 5600..5699 */

#define XMLERR_MissingSource                            5600
#define XMLERR_InvalidXml                               5601
#define XMLERR_InvalidXsd                               5602
#define XMLERR_XsdValidationFailed                      5603
#define XMLERR_MissingDependency                        5604



#define XSLERR_MissingSource                            5650
#define XSLERR_MissingXml                               5651
#define XSLERR_InvalidSource                            5652
#define XSLERR_InvalidXml                               5653
#define XSLERR_TargetBufferToSmall                      5654
#define XSLERR_CouldNotCreateTransform                  5655
#define XSLERR_TransformFailed                          5656
#define XSLERR_TransformError                           5657
#define XSLERR_ExternalFunctionIncompatible             5658
#define XSLERR_ExtenrnalFunctionMultipleURIs            5659

#endif
