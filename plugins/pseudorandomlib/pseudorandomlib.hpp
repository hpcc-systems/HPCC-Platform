/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#ifndef PSEUDO_RANDOMLIB_INCL
#define PSEUDO_RANDOMLIB_INCL

#ifdef _WIN32
#define PSEUDO_RANDOMLIB_CALL _cdecl
#ifdef PSEUDO_RANDOMLIB_EXPORTS
#define PSEUDO_RANDOMLIB_API __declspec(dllexport)
#else
#define PSEUDO_RANDOMLIB_API __declspec(dllimport)
#endif
#else
#define PSEUDO_RANDOMLIB_CALL
#define PSEUDO_RANDOMLIB_API
#endif

#include "hqlplugins.hpp"

extern "C" {
PSEUDO_RANDOMLIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
PSEUDO_RANDOMLIB_API void setPluginContext(IPluginContext * _ctx);
}

extern "C++"
{
PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberUniformDistribution(unsigned engine, unsigned lower_bound, unsigned upper_bound);
PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberBinomialDistribution(unsigned engine, const double probability, unsigned upper_bound);
PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberNegativeBinomialDistribution(unsigned engine, const double probability, unsigned upper_bound);
PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberGeometricDistribution(unsigned engine, const double probability);
PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberPoissonDistribution(unsigned engine, const double mean);
}

#endif
