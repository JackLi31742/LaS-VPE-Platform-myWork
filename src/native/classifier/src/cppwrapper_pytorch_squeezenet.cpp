#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
using namespace std;

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include "arrayobject.h"
#include "cppwrapper_pytorch_squeezenet.h"

PyTorchSqueezenetCppWrapper::PyTorchSqueezenetCppWrapper(void) {
    py_instance = NULL;
}

PyTorchSqueezenetCppWrapper::~PyTorchSqueezenetCppWrapper(void) {
    release();
}

int PyTorchSqueezenetCppWrapper::initPyEnv(const char* py_home, 
    const char* py_path) {
    if (py_home == NULL || py_path == NULL) {
        fprintf(stderr, "Python Home or python path is NULL!\n");
        return NULL_STR;
    }
    // Set python home ...
    size_t len = strlen(py_home);
    wchar_t* wc_python_home = Py_DecodeLocale(py_home, &len);
    Py_SetPythonHome(wc_python_home);
    // Set python path ...
    //setenv("PYTHONPATH","");
    //const char* c_python_path = "";
    //len = strlen(c_python_path);
    //wchar_t* wc_python_path = Py_DecodeLocale(c_python_path, &len);
    //Py_SetPath(wc_python_path);
    
    // Initialize the Python Interpreter
    printf("-99999999\n");
    Py_Initialize();
    printf("000000000\n");
    if (PyArray_API==NULL) {
        import_array();
    }
    // Set search path ...
    string python_path_self = py_path;
    //"/home/data/da.li/projects/ObjectDetection4j/src/native/lib";
    PyObject *sys = PyImport_ImportModule("sys");
    PyObject *path = PyObject_GetAttrString(sys, "path");
    PyList_Append(path, PyUnicode_FromString(python_path_self.c_str()));

    // Get python home.
    wchar_t* my_python_home = Py_GetPythonHome();
    char* mph = (char *)malloc( 1024 );
    len = wcstombs( mph, my_python_home, 1024);
    printf("%s\n", mph);
    // Get python path.
    wchar_t* python_path = Py_GetPath();
    len = wcstombs( mph, python_path, 1024);
    printf("%s\n", mph);
    free(mph);

    return 0;
}

int PyTorchSqueezenetCppWrapper::init(const char* weight_filename,
    const char* label_filename) {
    // Set python environments
    const char* py_home = "/home/vpetest.cripac/anaconda3";
    const char* py_path =
        "/home/vpetest.cripac/softwares/hadoop-2.7.4/lib/native";
    int ret = initPyEnv(py_home, py_path);
    if (ret < 0) {
        fprintf(stderr, "Set python environment FAILED!\n");
        return ret;
    }
    // Initialize
    char* input_module_name = (char*)"obj_classification";
    //char* input_func_name = (char*)"initialize";
    // Import module ...
    printf("111111111\n");
    PyObject* module_name = PyUnicode_DecodeFSDefault(input_module_name);
    printf("222222222\n");
    PyObject* py_module = PyImport_Import(module_name);
    printf("333333333\n");
    Py_DECREF(module_name);
    if (!py_module) {
        fprintf(stderr, "In init: Import %s FAILED!\n", input_module_name);
        return PY_MOD_IMPORT_ERR;
    } else {
        fprintf(stdout, "In init: %s import successfully!\n", input_module_name);
    }
   
    printf("L: %s .....\n", label_filename);
    printf("W: %s .....\n", weight_filename);
    // Import class.
    char* class_name = (char*)"ImgClassificationSqueezeNet";
    PyObject* py_dict = PyModule_GetDict(py_module);
    PyObject* py_class= PyDict_GetItemString(py_dict, class_name);
    // Create an instance of the class
    if (PyCallable_Check(py_class)) {
        PyObject* py_args = PyTuple_New(2);
        PyObject* py_val_label  = PyBytes_FromString((char*)label_filename);
        PyObject* py_val_weight = PyBytes_FromString((char*)weight_filename);
        PyTuple_SetItem(py_args, 0, py_val_label);
        PyTuple_SetItem(py_args, 1, py_val_weight);
        py_instance = PyObject_CallObject(py_class, py_args);
        if (!py_instance) {
            fprintf(stderr, "In init: create object FAILED!\n");
            Py_DECREF(py_args);
            return PY_CREATE_OBJ_ERR;
        }
        Py_DECREF(py_args);
    } else {
        fprintf(stderr, "Load class %s FAILED!\n", class_name);
        release();
        return PY_CLS_IMPORT_ERR;
    }
    fprintf(stdout, "Initialize successfully!\n");
    return SUCCESS;
}

int PyTorchSqueezenetCppWrapper::classify(unsigned char* data, int h, int w, 
    int c, char* cls) {
    if (!data) {
        fprintf(stderr, "In classify: input data is NULL!\n");
        return NULL_DATA;
    }

    if (py_instance) {
        PyObject* py_method_name = PyUnicode_FromString("run");
        // Convert cpp data to python object.
        int nd = 3;
        npy_intp dims[] = {h, w, c};
        PyObject* py_input_val = PyArray_SimpleNewFromData(nd, dims, NPY_UINT8,
            (void*)data);
        PyObject* py_args_in = PyTuple_New(1);
        PyTuple_SetItem(py_args_in, 0, py_input_val);
        PyObject* py_return_val = PyObject_CallMethodObjArgs(py_instance,
            py_method_name, py_args_in);
        Py_DECREF(py_args_in);
        if (py_return_val) {
            printf("Classification successfully!\n");
            // Parse results:
            if (!cls) {
                fprintf(stderr, 
                    "In classify: allocate memory to store object class\n");
                return NULL_DATA;
            }
            char* res = PyUnicode_AsUTF8(py_return_val);
            memcpy(cls, res, strlen(res)+1);
            Py_DECREF(py_return_val);
        } else {
            PyErr_Print();
            fprintf(stderr,"In classify: Call %s failed\n", "run");
            return PY_METHOD_ERR;
        }
    } else {
        fprintf(stderr, "In classify: Object Instance is NULL\n");
        return PY_METHOD_ERR;
    }
    return SUCCESS;
}

int PyTorchSqueezenetCppWrapper::release(void) {
    if (py_instance) {
        Py_DECREF(py_instance);
        py_instance = NULL;
    }
    if (Py_FinalizeEx() < 0) {
        return 120;
    }
    return 0;
}

#ifdef __cplusplus
extern "C" {
#endif

ImgClassificationIntf* create() {
    return new PyTorchSqueezenetCppWrapper;
}

void destroy(ImgClassificationIntf* obj) {
    delete obj;
    obj = NULL;
}

#ifdef __cplusplus
}
#endif
