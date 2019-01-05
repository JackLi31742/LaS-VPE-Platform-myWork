
#include <cstdlib>
#include <cstdio>
#include <string>
#include <dlfcn.h>

#include "img_classification_intf.hpp"
#include "org_cripac_isee_ImgClassification.h"

// static void* dl = NULL;

JNIEXPORT jlong JNICALL Java_org_cripac_isee_alg_pedestrian_classifier_ImgClassification_loadLibrary
    (JNIEnv* env, jobject obj, jstring lib_name) {
    const int kLibNameLen = env->GetStringUTFLength(lib_name);
    char* c_lib_name = new char[kLibNameLen+1];

    env->GetStringUTFRegion(lib_name, 0, kLibNameLen, c_lib_name);
    c_lib_name[kLibNameLen] = '\0';

    void* dl = dlopen(c_lib_name, RTLD_LAZY|RTLD_GLOBAL);
    if (!dl) {
        fprintf(stderr, "Load dll FAILED!\n");
        fprintf(stderr, "%s\n", dlerror());
        delete[] c_lib_name;
        c_lib_name = NULL;
        exit(-1);
    }
    delete[] c_lib_name;
    c_lib_name = NULL;

    return (jlong)dl;
}

JNIEXPORT jlong JNICALL Java_org_cripac_isee_alg_pedestrian_classifier_ImgClassification_initialize
    (JNIEnv *env, jobject obj, jlong jdl, jstring model_path, 
    jstring label_path) {
    // Model path
    const int kModelPathLen = env->GetStringUTFLength(model_path);
    char* c_model_path = new char[kModelPathLen + 1];
    env->GetStringUTFRegion(model_path, 0, kModelPathLen, c_model_path);
    c_model_path[kModelPathLen] = '\0';
    // Label path
    const int kLabelPathLen = env->GetStringUTFLength(label_path);
    char* c_label_path = new char[kLabelPathLen + 1];
    env->GetStringUTFRegion(label_path, 0, kLabelPathLen, c_label_path);
    c_label_path[kLabelPathLen] = '\0' ;   

    //PyTorchSSDCppWrapper* detector = new PyTorchSSDCppWrapper;
    void* dl = (void*)jdl;
    create_t create_func = (create_t) dlsym(dl, "create");
    if (!create_func) {
        fprintf(stderr, "Load sysmbol (create) FAILED!\n");
        delete[] c_model_path;
        c_model_path = NULL;
        delete[] c_label_path;
        c_label_path = NULL;
        return -1;
    }
    ImgClassificationIntf* classifier = create_func();

    int ret = classifier->init((const char*)c_model_path,
        (const char*)c_label_path);

    delete[] c_model_path;
    c_model_path = NULL;
    delete[] c_label_path;
    c_label_path = NULL;

    if (ret < 0) {
        fprintf(stderr, "Error: classifier initailize FAILED!\n");
        return (jlong)NULL;
    } else {
        return (jlong)classifier;
    }
}

JNIEXPORT jstring JNICALL Java_org_cripac_isee_alg_pedestrian_classifier_ImgClassification_classify
    (JNIEnv *env, jobject obj, jlong handle, jbyteArray jframe, 
    jint h, jint w, jint c) {
    ImgClassificationIntf* classifier = (ImgClassificationIntf *)handle;
    jbyte *frame = env->GetByteArrayElements(jframe, NULL);

    const int kMaxStrLen = 128;
    char* c_cls = new char[kMaxStrLen];

    int ret = classifier->classify((jboolean*)frame, h, w, c, c_cls);

    jstring j_cls = NULL;
    if (ret < 0) {
        fprintf(stderr, "Error: classifier classify FAILED!\n");
    } else {
        j_cls = env->NewStringUTF(c_cls);
    }

    delete[] c_cls;
    c_cls = NULL;

    return j_cls;
}

JNIEXPORT jint JNICALL Java_org_cripac_isee_alg_pedestrian_classifier_ImgClassification_release
    (JNIEnv *env, jobject obj, jlong handle, jlong jdl) {
    ImgClassificationIntf* classifier = (ImgClassificationIntf *)handle;
    int ret = -1;
    if (classifier) {
        ret = classifier->release();
    }
    void* dl = (void*)jdl;
    destroy_t destroy_func = (destroy_t) dlsym(dl, "destroy");
    if (!destroy_func) {
        fprintf(stderr, "Load sysmbol (destroy) FAILED!\n");
        dlclose(dl);
        exit(-1);
    }
    destroy_func(classifier);
 
    return ret;
}

JNIEXPORT void JNICALL Java_org_cripac_isee_alg_pedestrian_classifier_ImgClassification_closeLibrary
    (JNIEnv *env, jobject obj, jlong jdl) {
    void* dl = (void*)jdl;
    dlclose(dl);
}
