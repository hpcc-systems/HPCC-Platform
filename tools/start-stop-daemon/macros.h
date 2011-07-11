#ifndef LIBDPKG_MACROS_H
#define LIBDPKG_MACROS_H

/* Language definitions. */

#if HAVE_C_ATTRIBUTE
#define DPKG_ATTR_UNUSED    __attribute__((unused))
#define DPKG_ATTR_CONST     __attribute__((const))
#define DPKG_ATTR_NORET     __attribute__((noreturn))
#define DPKG_ATTR_PRINTF(n) __attribute__((format(printf, n, n + 1)))
#else
#define DPKG_ATTR_UNUSED
#define DPKG_ATTR_CONST
#define DPKG_ATTR_NORET
#define DPKG_ATTR_PRINTF(n)
#endif

#ifdef __cplusplus
#define DPKG_BEGIN_DECLS    extern "C" {
#define DPKG_END_DECLS      }
#else
#define DPKG_BEGIN_DECLS
#define DPKG_END_DECLS
#endif

#ifndef sizeof_array
#define sizeof_array(a) (sizeof(a) / sizeof((a)[0]))
#endif

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

#ifndef max
#define max(a, b) ((a) > (b) ? (a) : (b))
#endif

#endif /* LIBDPKG_MACROS_H */

