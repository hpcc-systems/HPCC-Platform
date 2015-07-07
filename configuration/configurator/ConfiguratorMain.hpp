#ifndef _CONFIGURATOR_MAIN_HPP_
#define _CONFIGURATOR_MAIN_HPP_

#include "ConfiguratorAPI.hpp"

void usage();

#ifdef CONFIGURATOR_LIB

    extern "C" int ConfiguratorMain(int argc, char* argv[]);

#endif // CONFIGURATOR_LIB



#endif // _CONFIGURATOR_MAIN_HPP_
