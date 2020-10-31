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



#define XPATHERR_InvalidState                           5680
#define XPATHERR_MissingInput                           5681
#define XPATHERR_InvalidInput                           5682
#define XPATHERR_UnexpectedInput                        5683
#define XPATHERR_EvaluationFailed                       5684


#endif
