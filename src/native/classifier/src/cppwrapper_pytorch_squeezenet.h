#ifndef _CPPWRAPPER_PYTORCH_SQUEEZENET_H_
#define _CPPWRAPPER_PYTORCH_SQUEEZENET_H_

#include "Python.h"
#include "img_classification_intf.hpp"

#define MAX_PEDESTRIAN_NUM 128


class PyTorchSqueezenetCppWrapper: public ImgClassificationIntf {

  public:
    PyTorchSqueezenetCppWrapper(void);
     ~PyTorchSqueezenetCppWrapper(void);

    // Initialization.
    int init(const char* weight_filename, const char* label_filename);
    // classification
    // Normal interface
    int classify(unsigned char* data, int h, int w, int c, char* cls);
    // Release.
    int release(void);

  private:
    int initPyEnv(const char* py_home, const char* py_path);

  private:
    PyObject* py_instance;
};

#endif // _CPPWRAPPER_PYTORCH_SQUEEZENET_H_

