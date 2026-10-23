/* Operation-local JNI resources and PostgreSQL/Java text conversion. */
#ifndef SYNCHDB_JAVA_JNI_H
#define SYNCHDB_JAVA_JNI_H

#include "postgres.h"
#include <jni.h>

/* Report and clear a pending Java exception. */
extern bool synchdb_java_exception(JNIEnv *env);
/* A successful begin must be paired with PopLocalFrame by its caller. */
extern bool synchdb_java_local_begin(JNIEnv *env, jint capacity);
/* Java failures return NULL; PostgreSQL encoding/allocation errors propagate. */
extern jstring synchdb_java_string(JNIEnv *env, const char *value);
/* Caller-owned database-encoding copy; rejects NUL and malformed UTF-16. */
extern char *synchdb_java_string_copy(JNIEnv *env, jstring value);

#endif /* SYNCHDB_JAVA_JNI_H */
