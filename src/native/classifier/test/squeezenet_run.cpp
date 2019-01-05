
#include <stdio.h>
#include <stdlib.h>

#include <dlfcn.h>

#include "img_classification_intf.hpp"

int main(int argc, char** argv) {
    
    void* dl = dlopen("libpytorch_squeezenet.so",RTLD_LAZY| RTLD_GLOBAL);
    if (!dl) {
        fprintf(stderr, "Load dll FAILED! - %s\n", dlerror());
        return -1;
    }
   
    create_t create_func = (create_t) dlsym(dl, "create");
    destroy_t destroy_func = (destroy_t) dlsym(dl, "destroy");
    if (!create_func || !destroy_func) {
        fprintf(stderr, "Load sysmbols FAILED\n");
        return -1;
    }
    ImgClassificationIntf* classifier = create_func();
    // Initialize ...
    int ret = classifier->init("/home/vpetest.cripac/softwares/hadoop-2.7.4/lib/native/classifier/squeezenet1_1-f364aa15.pth",
        "/home/vpetest.cripac/softwares/hadoop-2.7.4/lib/native/classifier/labels.json");
    if (ret < 0) {
        fprintf(stderr, "Initialization FAILED!\n");
        return ret;
    }
    // Detect ...
    //for (int i = 0; i < int(atoi(argv[2])); ++i) { 
    //    printf("======================== Num: %d\n", i);
    //    ret = detector->detect(argv[1]);
    //}
    // Release
    ret = classifier->release();
    destroy_func(classifier);
    dlclose(dl);
    return 0;
}
