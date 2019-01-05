
/**
 * DeepMARCaffe2NativeTest.java
 *
 * Test the java and jni code to call the model for Pedestrian Attributes Recognition.
 *
 * @Author  da.li
 * @Version 0.1 on 2017/10/16
 */

package org.cripac.isee.vpe.alg.pedestrian.attr;

import static org.bytedeco.javacpp.opencv_imgcodecs.IMREAD_COLOR;
import static org.bytedeco.javacpp.opencv_imgcodecs.imread;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffe2Native;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet.BoundingBox;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;

/**
 * Create by da.li on 2017/10/16
 */
public class DeepMARCaffe2NativeTest {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Error: BAD number of input args!");
            System.out.println("Info: 2 args are needed.");
            return;
        }

        System.out.println("Performing validness test...");
        System.out.println("Native library path: " + System.getProperty("java.library.path"));
        System.out.println("Creating DeepMAR ...");

        String gpu = "0";
        DeepMARCaffe2Native recognizer = new DeepMARCaffe2Native(
            gpu,
            new ConsoleLogger(Level.DEBUG));
        System.out.println("Create DeepMAR SUCCESSFULLY!");

        int cnt = 0;
        int total = Integer.parseInt(args[1]);
        final long start = System.currentTimeMillis();
        System.out.println("Load pedestrian data ...");
        String urlPedestrian = args[0];
        Tracklet tracklet = loadTracklet(urlPedestrian);
        if (tracklet == null) {
            System.out.println("Load pedestrian data FAILED!");
            return;
        }
        System.out.println("Pedestrian pair load successfully!");
        System.out.println("Start to recognize ...");
        while (cnt < total) {
            cnt++;
            System.out.println("======== Round " + cnt + " ========");

            // recognize ...
            final long startInside = System.currentTimeMillis();
            Attributes attr = recognizer.recognize(tracklet);
            //float dissimilarity = extracter.calDissimilarity(pedestrianA, pedestrianB);
            final long endInside = System.currentTimeMillis();
            long elapsedTime = endInside - startInside;
            System.out.println("Elapsed time of round " + cnt + " is " + elapsedTime + " ms");
        }
        final long end = System.currentTimeMillis();
        float totalTime = (end - start) / 1000.0f;
        System.out.println("Elapsed time is " + totalTime + " s");
    }

    // Load images.
    public static Tracklet loadTracklet(String storedLocalDir) {
        File f = new File(storedLocalDir);
        if (!f.exists()) {
            System.out.println(storedLocalDir + " not exists!");
            return null;
        }
        File files[] = f.listFiles();
        List<BoundingBox> samples = new ArrayList<>();
        for (int i = 0; i < files.length; ++i) {
            File fs = files[i];
            if (fs.isDirectory()) {
                continue;
            } else {
                BoundingBox bbox = new BoundingBox();
                String imagePath = storedLocalDir + "/" + fs.getName();
                opencv_core.Mat img = imread(imagePath, IMREAD_COLOR);
                if (img == null || img.empty()) {
                    System.out.println("Error: Load image FAILED!");
                    return null;
                }
                bbox.width = img.cols();
                bbox.height= img.rows();
                bbox.patchData = new byte[img.rows() * img.cols() * img.channels()];
                img.data().get(bbox.patchData);
                img.release();
                samples.add(bbox);
            }
        }
        BoundingBox[] bboxes = new BoundingBox[samples.size()];
        samples.toArray(bboxes);
        Tracklet tracklet = new Tracklet();
        tracklet.id.videoID = storedLocalDir;
        tracklet.id.serialNumber = 1;
        tracklet.locationSequence = bboxes;

        return tracklet;
    }
}
