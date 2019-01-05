
package org.cripac.isee.alg.pedestrian.detection;

import java.io.InputStream;

import javax.annotation.Nonnull;

public interface Detector {
    int process(byte[] frame, int h, int w, int c);
    void free();
    public void detection(@Nonnull InputStream videoStream) ;
}
