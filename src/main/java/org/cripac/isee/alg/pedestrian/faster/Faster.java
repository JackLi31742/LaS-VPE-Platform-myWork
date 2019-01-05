
package org.cripac.isee.alg.pedestrian.faster;

import java.io.InputStream;

import javax.annotation.Nonnull;

public interface Faster {
    int process(byte[] frame, int h, int w, int c);
    void free();
    public void faster(@Nonnull InputStream videoStream) ;
}
