#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "org_josh_JoshDB_FileTrie_MergeFile.h"

extern int errno;

JNIEXPORT jlong JNICALL Java_org_josh_JoshDB_FileTrie_MergeFile_openFile
(
  JNIEnv *env, 
  jclass clazz, 
  jstring path
)
{
  const char *nativePath = (*env)->GetStringUTFChars(env, path, 0);

  jlong fd = (jlong) open(nativePath, O_WRONLY | O_APPEND | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR);

  (*env)->ReleaseStringUTFChars(env, path, nativePath);

  if (-1 == fd)
  {
    fd = -errno;
  }

  return fd;
}

JNIEXPORT jlong JNICALL Java_org_josh_JoshDB_FileTrie_MergeFile_closeFile
(
  JNIEnv *env, 
  jclass clazz, 
  jlong fd
)
{
  if (-1 == close((int)fd))
  {
    return -errno;
  }

  return 0;
}


JNIEXPORT jlong JNICALL Java_org_josh_JoshDB_FileTrie_MergeFile_appendToFile
(
  JNIEnv *env, 
  jclass clazz, 
  jlong fd, 
  jbyteArray buffer, 
  jlong numBytes
)
{
  jsize native_num_bytes = (*env)->GetArrayLength(env, buffer);

  //they lied to us about size of buffer
  if (numBytes != native_num_bytes)
  {
    return -1;
  }

  jbyte *native_buffer = (*env)->GetByteArrayElements(env, buffer, NULL);

  // couldn't get contents of array, curious
  if (!native_buffer)
  {
    return -2;
  }

  ssize_t ret = write(fd, (void *)native_buffer, numBytes);
 
  if (ret < 0)
  {
    ret = -errno;
  }

  (*env)->ReleaseByteArrayElements(env, buffer, native_buffer, JNI_ABORT);

  return ret;
}


