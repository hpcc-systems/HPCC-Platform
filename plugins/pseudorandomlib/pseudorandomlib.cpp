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

#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#ifdef __APPLE__
	#include <mutex>
#endif
#include "pseudorandomlib.hpp"
#include "jutil.hpp"
#include "jmutex.hpp"

#define PSEUDO_RANDOMLIB_VERSION "PSEUDO_RANDOMLIB 1.0.00"

static const char * HoleDefinition = NULL;

static const char * EclDefinition =
"export RandomLib := SERVICE\n"
"  UNSIGNED4 nextUniformRandom(UNSIGNED4 engine, UNSIGNED4 lower_bound, UNSIGNED4 upper_bound) : cpp,entrypoint='prGetNextPseudoRandomNumberUniformDistribution'; \n"
"  UNSIGNED4 nextBinomialRandom(UNSIGNED4 engine, CONST REAL8 probability, UNSIGNED4 upper_bound) : cpp,entrypoint='prGetNextPseudoRandomNumberBinomialDistribution'; \n"
"  UNSIGNED4 nextNegativeBinomialRandom(UNSIGNED4 engine, CONST REAL8 probability, UNSIGNED4 upper_bound) : cpp,entrypoint='prGetNextPseudoRandomNumberNegativeBinomialDistribution'; \n"
"  UNSIGNED4 nextGeometricRandom(UNSIGNED4 engine, CONST REAL8 probability) : cpp,entrypoint='prGetNextPseudoRandomNumberGeometricDistribution'; \n"
"  UNSIGNED4 nextPoissonRandom(UNSIGNED4 engine, CONST REAL8 mean) : cpp,entrypoint='prGetNextPseudoRandomNumberPoissonDistribution'; \n"
"END;";

PSEUDO_RANDOMLIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = PSEUDO_RANDOMLIB_VERSION " $Revision: 62376 $";
    pb->moduleName = "lib_pseudorandomlib";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "PsuedoRandomLib pseudorandom services library";
    return true;
}

namespace nsPseudoRandomLib {
    IPluginContext * parentCtx = NULL;
}
using namespace nsPseudoRandomLib;

PSEUDO_RANDOMLIB_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

//-------------------------------------------------------------------------------------------------------------------------------------------

namespace nsPseudoRandomLib
{

#ifdef __APPLE__
__thread  Owned<IPseudoRandomNumberGenerator> pseudorandom_ = createPseudoRandomNumberGenerator();
#else
thread_local Owned<IPseudoRandomNumberGenerator> pseudorandom_ = createPseudoRandomNumberGenerator();
#endif

PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberUniformDistribution(unsigned engine, unsigned lower_bound, unsigned upper_bound)
{
    return pseudorandom_->nextUniform(static_cast<IPseudoRandomNumberGenerator::ePseudoRandomNumberEngine>(engine), lower_bound, upper_bound);
}

PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberBinomialDistribution(unsigned engine, const double probability, unsigned upper_bound)
{
    return pseudorandom_->nextBinomial(static_cast<IPseudoRandomNumberGenerator::ePseudoRandomNumberEngine>(engine), probability, upper_bound);
}

PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberNegativeBinomialDistribution(unsigned engine, const double probability, unsigned upper_bound)
{
    return pseudorandom_->nextNegativeBinomial(static_cast<IPseudoRandomNumberGenerator::ePseudoRandomNumberEngine>(engine), probability, upper_bound);
}

PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberGeometricDistribution(unsigned engine, const double probability)
{
    return pseudorandom_->nextGeometric(static_cast<IPseudoRandomNumberGenerator::ePseudoRandomNumberEngine>(engine), probability);
}

PSEUDO_RANDOMLIB_API unsigned PSEUDO_RANDOMLIB_CALL prGetNextPseudoRandomNumberPoissonDistribution(unsigned engine, const double mean)
{
    return pseudorandom_->nextPoisson(static_cast<IPseudoRandomNumberGenerator::ePseudoRandomNumberEngine>(engine), mean);
}
}
