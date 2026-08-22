#ifndef FLOWMQ_EXPORT_H
#define FLOWMQ_EXPORT_H

/* CMake supplies FLOWMQ_API for Windows shared-library producers and consumers. */
#ifndef FLOWMQ_API
  #if !defined(_WIN32) && defined(__GNUC__) && __GNUC__ >= 4
    #define FLOWMQ_API __attribute__((visibility("default")))
  #else
    #define FLOWMQ_API
  #endif
#endif

#ifndef FLOWMQ_C_API
  #ifdef __cplusplus
    #define FLOWMQ_C_API extern "C" FLOWMQ_API
  #else
    #define FLOWMQ_C_API FLOWMQ_API
  #endif
#endif

#endif /* FLOWMQ_EXPORT_H */
