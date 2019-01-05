package org.cripac.isee.alg.pedestrian.classifier;

import java.io.InputStream;

import javax.annotation.Nonnull;

import org.bytedeco.javacv.FrameGrabber.Exception;

public interface Classifier {
    String process(byte[] frame, int h, int w, int c);
    void free();
    public void classifier(@Nonnull InputStream videoStream) ;
}
