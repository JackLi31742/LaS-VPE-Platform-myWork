#include <nvml.h>
#include "cuda_monitor4j.h"

inline nvmlDevice_t getDevice(unsigned int index) {
  nvmlDevice_t dev;
  nvmlDeviceGetHandleByIndex(index, &dev);
  return dev;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getInfoCount
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getInfoCount
(JNIEnv *env, jobject obj, jint index){
unsigned int infoCount;
nvmlProcessInfo_t infos[10];
nvmlDeviceGetComputeRunningProcesses(getDevice((unsigned int) index),&infoCount,infos);
	return infoCount;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getPid
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getPid
  (JNIEnv *env, jobject obj, jint index, jint num){
unsigned int infoCount;
nvmlProcessInfo_t infos[10];
nvmlDeviceGetComputeRunningProcesses(getDevice((unsigned int) index),&infoCount,infos);
	return infos[num].pid;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getUsedGpuMemory
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getUsedGpuMemory
  (JNIEnv *env, jobject obj, jint index, jint num){
unsigned int infoCount;
nvmlProcessInfo_t infos[10];
nvmlDeviceGetComputeRunningProcesses(getDevice((unsigned int) index),&infoCount,infos);
	return infos[num].usedGpuMemory;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    initNVML
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_initNVML
    (JNIEnv *env, jobject obj) {
  return nvmlInit();
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getDeviceCount
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getDeviceCount
    (JNIEnv *env, jobject obj) {
  unsigned int cnt;
  nvmlDeviceGetCount(&cnt);
  return cnt;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getFanSpeed
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getFanSpeed
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int fanSpeed;
  nvmlDeviceGetFanSpeed(getDevice((unsigned int) index), &fanSpeed);
  return fanSpeed;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getUtilizationRate
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getUtilizationRate
    (JNIEnv *env, jobject obj, jint index) {
  nvmlUtilization_t rates;
  nvmlDeviceGetUtilizationRates(getDevice((unsigned int) index), &rates);
  return rates.gpu;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getFreeMemory
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getFreeMemory
    (JNIEnv *env, jobject obj, jint index) {
  nvmlMemory_t memory;
  nvmlDeviceGetMemoryInfo(getDevice((unsigned int) index), &memory);
  return (jlong) memory.free;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getTotalMemory
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getTotalMemory
    (JNIEnv *env, jobject obj, jint index) {
  nvmlMemory_t memory;
  nvmlDeviceGetMemoryInfo(getDevice((unsigned int) index), &memory);
  return (jlong) memory.total;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getUsedMemory
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getUsedMemory
    (JNIEnv *env, jobject obj, jint index) {
  nvmlMemory_t memory;
  nvmlDeviceGetMemoryInfo(getDevice((unsigned int) index), &memory);
  return (jlong) memory.used;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getTemperature
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getTemperature
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int temperature;
  nvmlDeviceGetTemperature(getDevice((unsigned int) index), NVML_TEMPERATURE_GPU, &temperature);
  return temperature;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getSlowDownTemperatureThreshold
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getSlowDownTemperatureThreshold
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int thresh;
  nvmlDeviceGetTemperatureThreshold(getDevice((unsigned int) index), NVML_TEMPERATURE_THRESHOLD_SLOWDOWN, &thresh);
  return thresh;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getShutdownTemperatureThreshold
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getShutdownTemperatureThreshold
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int thresh;
  nvmlDeviceGetTemperatureThreshold(getDevice((unsigned int) index), NVML_TEMPERATURE_THRESHOLD_SHUTDOWN, &thresh);
  return thresh;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getPowerLimit
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getPowerLimit
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int limit;
  nvmlDeviceGetEnforcedPowerLimit(getDevice((unsigned int) index), &limit);
  return limit;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_GpuManage
 * Method:    getPowerUsage
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_GpuManage_getPowerUsage
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int usage;
  nvmlDeviceGetPowerUsage(getDevice((unsigned int) index), &usage);
  return usage;
}