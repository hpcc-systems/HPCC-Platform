#ifndef SACOALESCER_HPP
#define SACOALESCER_HPP

#include "sautil.hpp"

extern sashalib_decl ISashaServer *createSashaSDSCoalescingServer();
extern sashalib_decl void suspendCoalescingServer();
extern sashalib_decl void resumeCoalescingServer();
extern sashalib_decl void coalesceDatastore(IPropertyTree *config, bool force);

#endif
