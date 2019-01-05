
/**
 * img_classification_intf.hpp
 **/

#ifndef _IMG_CLASSIFICATION_INTF_HPP_

typedef enum img_cls_err_type_t_ {
    SUCCESS = 0,
    FILE_NOT_EXIST = -1,
    PY_MOD_IMPORT_ERR = -2,
    PY_CLS_IMPORT_ERR = -3,
    PY_CREATE_OBJ_ERR = -4,
    PY_METHOD_ERR = -5,
    NULL_STR = -6,
    NULL_DATA= -7,
} ImgClsErrorTypes;

class ImgClassificationIntf {
  public:
    virtual ~ImgClassificationIntf() {};
    
    // Initialization.
    virtual int init(const char* weight_filename, 
        const char* label_filename) = 0;
    // Detection
    virtual int classify(unsigned char* data, int h, int w, int c, 
        char* cls) = 0;
    // Release
    virtual int release(void) = 0;
};

#ifdef __cplusplus
extern "C" { 
#endif

typedef ImgClassificationIntf* (*create_t)();
typedef void (*destroy_t)(ImgClassificationIntf*);

#ifdef __cplusplus
}
#endif

#endif  // __IMG_CLASSIFICATION_INTF_HPP_ 
