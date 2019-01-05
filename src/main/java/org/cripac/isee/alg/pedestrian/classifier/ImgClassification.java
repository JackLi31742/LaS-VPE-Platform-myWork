package org.cripac.isee.alg.pedestrian.classifier;

import static org.bytedeco.javacpp.avutil.AV_LOG_QUIET;
import static org.bytedeco.javacpp.avutil.av_log_set_level;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.CharacterCodingException;
import java.nio.file.AccessDeniedException;

import javax.annotation.Nullable;

import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.FrameGrabber.Exception;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

public class ImgClassification implements Classifier {

    static {
        try {
            System.out.println("Load native libraries from " + System.getProperty("java.library.path"));
            System.loadLibrary("cppwrapper_pytorch_squeezenet_jni");
            System.out.println("Native library loaded successfully!");
        } catch (Throwable t) {
            System.out.println("Failed to load native library!");
            t.printStackTrace();
        }
        
        try {
        	Loader.load(opencv_core.class);
			
		} catch (Throwable e) {
			// TODO: handle exception
			System.out.println("Failed to load opencv_core!");
			e.printStackTrace();
		}
    }

    private long handle;
    private long dl;
    private Logger logger;
    
    private native long loadLibrary(String lib_name);
    private native long initialize(long dl, String modelPath, String labelPath);
    private native String classify(long handle, byte[] imgBuf, int h, int w, int c);
    private native int release(long handle, long dl);
    private native void closeLibrary(long dl);

    // Constructor
    public ImgClassification(File modelFile, File labelFile,@Nullable Logger logger) throws 
        FileNotFoundException, AccessDeniedException, CharacterCodingException {
        
        if (!modelFile.exists()) {
            throw new FileNotFoundException("Cannot find " + 
                modelFile.getPath());
        }
        if (!modelFile.canRead()) {
            throw new AccessDeniedException("Cannot read " + 
                modelFile.getPath());
        }
        if (!labelFile.exists()) {
            throw new FileNotFoundException("Cannot find " + 
                labelFile.getPath());
        }
        if (!labelFile.canRead()) {
            throw new AccessDeniedException("Cannot read " + 
                labelFile.getPath());
        }
        
        // Load library.
        dl = loadLibrary("libpytorch_squeezenet.so");
        if (dl > 0) {
            System.out.println("Load library of python code Done!");
        } else {
            System.exit(-1);
        }
        System.out.println("Initializing ...");
        // Initialize.
        handle = initialize(dl, modelFile.getPath(), labelFile.getPath());
        if (handle > 0) {
            System.out.println("Initialization Done!");
        } else {
            System.out.println("Initialize FAILED!");
            System.exit(-1);
        }
        
        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }
        
    }

    @Override
    public void free() {
        release(handle, dl);
        closeLibrary(dl);
        //super.finalize();
        System.out.println("Resources RELEASED!");
    }

    @Override
    public String process(byte[] frame, int h, int w, int c) {
        String category = classify(handle, frame, h, w, c);
        return category; // Test
    }

    @Override
    protected void finalize() throws Throwable {
        //release(handle, dl);
        //closeLibrary(dl);
        super.finalize();
        //System.out.println("Resources RELEASED!");
    }
	@Override
	public void classifier(InputStream videoStream)  {
    	FFmpegFrameGrabber frameGrabber = new FFmpegFrameGrabber(videoStream);
        av_log_set_level(AV_LOG_QUIET);
        try {
			frameGrabber.start();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        logger.debug("Initialized video decoder!");
        
        int cnt = 0;
        // Every time a frame is retrieved during decoding, it is immediately fed into the tracker,
        // so as to save runtime memory.
        while (true) {
            Frame frame;
            try {
                frame = frameGrabber.grabImage();
            } catch (FrameGrabber.Exception e) {
                logger.error("On grabImage: " + e);
                break;
            }
            if (frame == null) {
                break;
            }
            final byte[] buf = new byte[frame.imageHeight * frame.imageWidth * frame.imageChannels];
            opencv_core.Mat cvFrame  =null;
            try {
            	cvFrame  = new OpenCVFrameConverter.ToMat().convert(frame);
				
            	cvFrame.data().get(buf);
            	String res =process(buf, cvFrame.rows(), cvFrame.cols(), cvFrame.channels());
            	if (res==null||res=="") {
            		break;
            	}
            	++cnt;
            	if (cnt % 1000 == 0) {
            		logger.debug("classifier " + cnt + " frames!");
            	}
			} 
            finally {
				// TODO: handle finally clause
            	if (cvFrame!=null) {
					
            		cvFrame.release();
				}
			}
        }

        logger.debug("Totally classifier " + cnt + " framed!");
//        try {
//			frameGrabber.close();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
        
    }
}
