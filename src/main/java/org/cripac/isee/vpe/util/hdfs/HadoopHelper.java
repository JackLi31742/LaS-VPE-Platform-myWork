/*
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cripac.isee.vpe.util.hdfs;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imdecode;
import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.codec.binary.Base64;  // Add by da.li.
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.util.logging.Logger;
import org.spark_project.guava.collect.ContiguousSet;
import org.spark_project.guava.collect.DiscreteDomain;
import org.spark_project.guava.collect.Range;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;

/**
 * The HadoopHelper class provides utilities for Hadoop usage.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class HadoopHelper {

	public static Singleton<FileSystem> fsSingleton;
    static {
        // These two lines are used to solve the following problem:
        // RuntimeException: No native JavaCPP library
        // in memory. (Has Loader.load() been called?)
        Loader.load(org.bytedeco.javacpp.helper.opencv_core.class);
        Loader.load(opencv_imgproc.class);
        try {
			fsSingleton=new Singleton<>(()->{FileSystem fs=getFileSystem();return fs;},FileSystem.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    public static FileSystem hdfs ;
    public static FileSystem getFileSystem(){
    	try {
			hdfs =new HDFSFactory().produce();
//			hdfs = FileSystem.get(new URI("hdfs://cpu-master-nod:8020"), getDefaultConf());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return hdfs;
    }
    public static Configuration getDefaultConf() {
        // Load Hadoop configuration from XML files.
        Configuration hadoopConf = new Configuration();
        String hadoopHome = System.getenv("HADOOP_HOME");
        hadoopConf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
        hadoopConf.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
        hadoopConf.setBoolean("dfs.support.append", true);
        hadoopConf.set("fs.hdfs.impl", DistributedFileSystem.class.getName(), "LaS-VPE Platform");
        hadoopConf.set("fs.file.impl", LocalFileSystem.class.getName(), "LaS-VPE Platform");
        return hadoopConf;
    }

    /**
     * Retrieve a tracklet from the HDFS or HAR.
     * Since a tracklet might be deleted from HDFS during reading,
     * it is highly recommended to retry this function on failure,
     * and the next time it will find the tracklet from HAR.
     *
     * @param storeDir the directory storing the tracklet (including only data of this tracklet).
     * @return the track retrieved.
     * @throws IOException        on failure of retrieving the tracklet.
     * @throws URISyntaxException on syntax error detected in the storeDir.
     */
    @Nonnull
    public static Tracklet retrieveTracklet(@Nonnull String storeDir) throws IOException, URISyntaxException {
        return retrieveTracklet(storeDir, new HDFSFactory().produce());
    }

    /**
     * Retrieve a tracklet from the HDFS or HAR.
     * Since a tracklet might be deleted from HDFS during reading,
     * it is highly recommended to retry this function on failure,
     * and the next time it will find the tracklet from HAR.
     *
     * @param storeDir the directory storing the tracklet (including only data of this tracklet).
     * @return the track retrieved.
     * @throws IOException        on failure of retrieving the tracklet.
     * @throws URISyntaxException on syntax error detected in the storeDir.
     */
    @Nonnull
    public static Tracklet retrieveTracklet(@Nonnull String storeDir,
                                            @Nonnull FileSystem hdfs) throws IOException, URISyntaxException {
        final InputStreamReader infoReader;
        final HarFileSystem harFS;
        final FileSystem fs;
        final String revisedStoreDir;

        boolean onHDFS = false;
        try {
            onHDFS = hdfs.exists(new Path(storeDir));
        } catch (IOException | IllegalArgumentException ignored) {
        }
        if (onHDFS) {
            infoReader = new InputStreamReader(hdfs.open(new Path(storeDir + "/info.txt")));
            fs = hdfs;
            revisedStoreDir = storeDir;
            harFS = null;
        } else {
            // Open the Hadoop Archive of the task the track is generated in.
            while (storeDir.endsWith("/")) {
                storeDir = storeDir.substring(0, storeDir.length() - 1);
            }
            if (storeDir.contains(".har")) {
                revisedStoreDir = storeDir;
            } else {
                final int splitter = storeDir.lastIndexOf("/");
                revisedStoreDir = storeDir.substring(0, splitter) + ".har" + storeDir.substring(splitter);
            }
            harFS = new HarFileSystem();
            harFS.initialize(new URI(revisedStoreDir), new Configuration());
            infoReader = new InputStreamReader(hdfs.open(new Path(revisedStoreDir + "/info.txt")));
            fs = harFS;
        }

        // Read verbal informations of the track.
        Gson gson = new Gson();
        Tracklet tracklet = gson.fromJson(infoReader, Tracklet.class);

        // Read frames concurrently..
        ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
                .parallelStream()
                .forEach(idx -> {
                    Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];
                    FSDataInputStream imgInputStream;
                    final Path imgPath = new Path(revisedStoreDir + "/" + idx + ".jpg");
                    boolean isSample = false;
                    try {
                        isSample = fs.exists(imgPath);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {
                        if (isSample) {
                            imgInputStream = fs.open(imgPath);
                            byte[] rawBytes = IOUtils.toByteArray(imgInputStream);
                            imgInputStream.close();
                            opencv_core.Mat img = imdecode(new opencv_core.Mat(rawBytes), 1); // CV_LOAD_IMAGE_COLOR=1
                            bbox.patchData = new byte[img.rows() * img.cols() * img.channels()];
                            img.data().get(bbox.patchData);
                            img.release();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        if (harFS != null) {
//            harFS.close();
        }
        return tracklet;
    }

    /**
     * Get the content of info.txt in hdfs of Har.
     * 
     * @param  storeDir the directory storing the tracklet.
     * @throws IOException        on failure of retrieving the tracklet.
     * @throws URISyntaxException on syntax error detected in the storeDir.
     * @return the content in info.txt which is in json format (as a string).
     */
    public static String getTrackletInfo(@Nonnull String storeDir) throws IOException, URISyntaxException {
        final InputStreamReader infoReader;
        final HarFileSystem harFS;
        final String revisedStoreDir;

        FileSystem hdfs = new HDFSFactory().produce();

        boolean onHDFS = false;
        try {
            onHDFS = hdfs.exists(new Path(storeDir));
        } catch(IOException | IllegalArgumentException ignored) {
            
        }
        if (onHDFS) {
            infoReader = new InputStreamReader(hdfs.open(new Path(storeDir + "/info.txt")));
            harFS = null;
        } else {
            // Open the Hadoop Archive of the task the track is generated in.
            while (storeDir.endsWith("/")) {
                storeDir = storeDir.substring(0, storeDir.length() - 1);
            }
            if (storeDir.contains(".har")) {
                revisedStoreDir = storeDir;
            } else {
                final int splitter = storeDir.lastIndexOf("/");
                revisedStoreDir = storeDir.substring(0, splitter) + ".har" + storeDir.substring(splitter);
            }
            harFS = new HarFileSystem();
            // When we run the code on our platform, it maybe not necessary to call getDefaultConf().
            // And just use "new Configuration()" as the second input in harFS.initialize().
            //Configuration hdfsConf = getDefaultConf();
            //harFS.initialize(new URI(revisedStoreDir), hdfsConf);
            harFS.initialize(new URI(revisedStoreDir), new Configuration());
            infoReader = new InputStreamReader(harFS.open(new Path(revisedStoreDir + "/info.txt")));
        }

        BufferedReader bufferedReader = new BufferedReader(infoReader);
        String trackletInfo = bufferedReader.readLine();

        if (harFS != null) {
//            harFS.close();
        }

        return trackletInfo;
    }

    /**
     * Get the content of info.txt in hdfs.
     * 
     * @param  storeDir the directory storing the tracklet.
     * @throws IOException        on failure of retrieving the tracklet.
     * @throws URISyntaxException on syntax error detected in the storeDir.
     * @return the content in info.txt which is in json format (as a string).
     */
	public static String getTrackletInfo(@Nonnull String storeDir, @Nonnull FileSystem hdfs) {
		boolean onHDFS = false;
		try {
			onHDFS = hdfs.exists(new Path(storeDir));
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (onHDFS) {
			InputStreamReader infoReader = null;
			String trackletInfo = "";
			BufferedReader bufferedReader = null;
			try {
				infoReader = new InputStreamReader(hdfs.open(new Path(storeDir + "/info.txt")));
				bufferedReader = new BufferedReader(infoReader);
				trackletInfo = bufferedReader.readLine();

			} catch (Exception e) {
				// TODO: handle exception
			} finally {

				try {
					if (bufferedReader != null) {
						bufferedReader.close();
						bufferedReader=null;
					}
					if (infoReader!=null) {
						infoReader.close();
						infoReader=null;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			return trackletInfo;
		} else {
			return null;
		}
	}

    /**
     * Store a tracklet to the HDFS.
     *
     * @param storeDir the directory storing the tracklet.
     * @param tracklet the tracklet to store.
     * @throws IOException on failure creating and writing files in HDFS.
     */
    public static void storeTracklet(@Nonnull String storeDir,
                                     @Nonnull Tracklet tracklet,
                                     @Nonnull FileSystem hdfs) throws Exception {
        // Write verbal informations with Json.
        final FSDataOutputStream outputStream = hdfs.create(new Path(storeDir + "/info.txt"));

        // Customize the serialization of bounding box in order to ignore patch data.
        final GsonBuilder gsonBuilder = new GsonBuilder();
        final JsonSerializer<Tracklet.BoundingBox> bboxSerializer = (box, typeOfBox, context) -> {
            JsonObject result = new JsonObject();
            result.add("x", new JsonPrimitive(box.x));
            result.add("y", new JsonPrimitive(box.y));
            result.add("width", new JsonPrimitive(box.width));
            result.add("height", new JsonPrimitive(box.height));
            return result;
        };
        gsonBuilder.registerTypeAdapter(Tracklet.BoundingBox.class, bboxSerializer);

        // Write serialized basic information of the tracklet to HDFS.
        outputStream.writeBytes(gsonBuilder.create().toJson(tracklet));
        outputStream.close();

        // Write frames concurrently.
        ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
                .parallelStream()
                // Find bounding boxes that contain patch data.
                .filter(idx -> tracklet.locationSequence[idx].patchData != null)
                .forEach(idx -> {
                    final Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];

                    // Use JavaCV to encode the image patch
                    // into JPEG, stored in the memory.
                    final BytePointer inputPointer = new BytePointer(bbox.patchData);
                    final opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
                    final BytePointer outputPointer = new BytePointer();
                    imencode(".jpg", image, outputPointer);
                    final byte[] bytes = new byte[(int) outputPointer.limit()];
                    outputPointer.get(bytes);

                    // Output the image patch to HDFS.
                    final FSDataOutputStream imgOutputStream;
                    try {
                        imgOutputStream = hdfs.create(new Path(storeDir + "/" + idx + ".jpg"));
                        imgOutputStream.write(bytes);
                        imgOutputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    // Free resources.
                    image.release();
                    inputPointer.deallocate();
                    outputPointer.deallocate();
                });
    }
    
	public static void storeTracklet(String nodeID, @Nonnull String storeDir, @Nonnull Tracklet tracklet,
			@Nonnull FileSystem hdfs
			// , GraphDatabaseConnector dbConnector
			// ,TrackletImgEntity trackletImgEntity
			, Logger logger) throws Exception {
		// Write verbal informations with Json.
		final FSDataOutputStream outputStream = hdfs.create(new Path(storeDir + "/info.txt"));
		// final FSDataOutputStream outputStreamBytes = hdfs.create(new
		// Path(storeDir + "/bytes.txt"));

		// Customize the serialization of bounding box in order to ignore patch
		// data.
		final GsonBuilder gsonBuilder = new GsonBuilder();
		final JsonSerializer<Tracklet.BoundingBox> bboxSerializer = (box, typeOfBox, context) -> {
			JsonObject result = new JsonObject();
			result.add("x", new JsonPrimitive(box.x));
			result.add("y", new JsonPrimitive(box.y));
			result.add("width", new JsonPrimitive(box.width));
			result.add("height", new JsonPrimitive(box.height));
			return result;
		};
		gsonBuilder.registerTypeAdapter(Tracklet.BoundingBox.class, bboxSerializer);

		// Write serialized basic information of the tracklet to HDFS.
		outputStream.writeBytes(gsonBuilder.create().toJson(tracklet));
		outputStream.close();
		int[] widths = new int[tracklet.locationSequence.length];
		byte[][] outBytes = new byte[tracklet.locationSequence.length][];
		// byte[] outBytes=null;
		// Write frames concurrently.
		ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
				.parallelStream()
				// Find bounding boxes that contain patch data.
				.filter(idx -> tracklet.locationSequence[idx].patchData != null).forEach((idx) -> {
					final Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];

					// Use JavaCV to encode the image patch
					// into JPEG, stored in the memory.
					final BytePointer inputPointer = new BytePointer(bbox.patchData);
					widths[idx] = bbox.width;
					final opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
					final BytePointer outputPointer = new BytePointer();
					imencode(".jpg", image, outputPointer);
					final byte[] bytes = new byte[(int) outputPointer.limit()];
					outputPointer.get(bytes);
					// logger.info(storeDir+"/bytes:"+bytes.length);
					// logger.info(storeDir+"/bytes:"+Arrays.toString(bytes));
					outBytes[idx] = bytes;
					// logger.info(storeDir+"/outBytes:"+outBytes[idx].length);
					// trackletImgEntity.setOutBytes(outBytes);
					// Free resources.
					// image.release();
					// inputPointer.deallocate();
					// outputPointer.deallocate();
				});

		/*
		 * byte[] out=null; for (int i = 0; i < outBytes.length; i++) { if
		 * (outBytes[i]!=null) {
		 * 
		 * out=outBytes[0]; }else{ return; } } for (int j = 1; j <
		 * outBytes.length; j++) {
		 * logger.info(storeDir+j+"/outBytes:"+outBytes[j].length); if
		 * (out!=null) {
		 * 
		 * out=ArrayUtils.addAll(out, outBytes[j]); } }
		 */
		// byte[] outBytes2;

		// Output the image patch to HDFS.
		final FSDataOutputStream imgOutputStream;
		try {
			imgOutputStream = hdfs.create(new Path(storeDir + "/" + "bbox.data"));
			// imgOutputStream.write(outBytes2);
			for (int i = 0; i < outBytes.length; i++) {
				if (outBytes[i] != null) {
					// logger.info(storeDir+"/outBytes:"+outBytes[i].length);
					// logger.info(storeDir+"/outBytes:"+Arrays.toString(outBytes[i]));
					imgOutputStream.write(("x=" + String.valueOf(tracklet.locationSequence[i].x)).getBytes("utf-8"));
					imgOutputStream.write(" ".getBytes("utf-8"));
					imgOutputStream.write(("y=" + String.valueOf(tracklet.locationSequence[i].y)).getBytes("utf-8"));
					imgOutputStream.write(" ".getBytes("utf-8"));
					imgOutputStream
							.write(("width=" + String.valueOf(tracklet.locationSequence[i].width)).getBytes("utf-8"));
					imgOutputStream.write(" ".getBytes("utf-8"));
					imgOutputStream
							.write(("height=" + String.valueOf(tracklet.locationSequence[i].height)).getBytes("utf-8"));
					imgOutputStream.write(System.getProperty("line.separator").getBytes("utf-8"));
					imgOutputStream.write(Arrays.toString(outBytes[i]).getBytes("utf-8"));
					imgOutputStream.write(System.getProperty("line.separator").getBytes("utf-8"));
					imgOutputStream.flush();

				} else {
					return;
				}
			}
			imgOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("------------------------------------------------");
		// dbConnector.saveTrackletImg(nodeID, widths);
	}


    /**
     * Store a tracklet to the HDFS.
     *
     * @param storeDir the directory storing the tracklet.
     * @param tracklet the tracklet to store.
     * @throws IOException on failure creating and writing files in HDFS.
     *
     * return info of the bounding boxes in json format.
     */
    
    public static String storeTrackletNew(@Nonnull String storeDir,
                                          @Nonnull Tracklet tracklet,
                                          @Nonnull FileSystem hdfs)  {
        // Write the tracklet data to a file.
    	FSDataOutputStream imgOutputStream =null;
    	BufferedWriter bw =null;
    	if (hdfs==null) {
    		hdfs=HDFSFactory.newInstance();
    	}
    	try {
    		 imgOutputStream = hdfs.create(new Path(storeDir + "/tracklet.data"));
    		 bw = new BufferedWriter(new OutputStreamWriter(imgOutputStream));
			
        // Length of a trajectory.
        int trackLength = tracklet.locationSequence.length;
        for (int ti = 0; ti < trackLength; ++ti) {
            final Tracklet.BoundingBox bbox = tracklet.locationSequence[ti];
            if (bbox.patchData != null) {
                String base64String = Base64.encodeBase64String(bbox.patchData);
                Map<String,String> trackletMap = new HashMap<String, String>();
                trackletMap.put("idx", ""+ti);
                trackletMap.put("x", ""+bbox.x);
                trackletMap.put("y", ""+bbox.y);
                trackletMap.put("width", ""+bbox.width);
                trackletMap.put("height", ""+bbox.height);
                trackletMap.put("data", base64String);
                Gson gson = new Gson();
                String json = gson.toJson(trackletMap);
                bw.write(json);
                bw.newLine();
            }
        }
        bw.flush();
    	} catch (Exception e) {
    		// TODO: handle exception
    		e.printStackTrace();
    	}
    	/*finally {
			try {
				if (bw!=null) {
					
					bw.close();
					bw=null;
				}
				if (imgOutputStream!=null) {
					
					imgOutputStream.close();
					imgOutputStream=null;
				}
			} catch (Exception e2) {
				// TODO: handle exception
				e2.printStackTrace();
			}
		}*/

        // Write serialized basic information of the tracklet to HDFS.
        // Customize the serialization of bounding box in order to ignore patch data.
        final GsonBuilder gsonBuilder = new GsonBuilder();
        final JsonSerializer<Tracklet.BoundingBox> bboxSerializer = (box, typeOfBox, context) -> {
            JsonObject result = new JsonObject();
            result.add("x", new JsonPrimitive(box.x));
            result.add("y", new JsonPrimitive(box.y));
            result.add("width", new JsonPrimitive(box.width));
            result.add("height", new JsonPrimitive(box.height));
            return result;
        };
        gsonBuilder.registerTypeAdapter(Tracklet.BoundingBox.class, bboxSerializer);
        FSDataOutputStream infoOutputStream =null;
        if (hdfs==null) {
    		hdfs=HDFSFactory.newInstance();
    	}
        try {
        	
        	infoOutputStream = hdfs.create(new Path(storeDir + "/info.txt"));
        	infoOutputStream.writeBytes(gsonBuilder.create().toJson(tracklet));
        	infoOutputStream.flush();
        } catch (Exception e) {
        	// TODO: handle exception
        	e.printStackTrace();
        }
        /*finally {
			try {
				if (infoOutputStream!=null) {
					
					infoOutputStream.close();
					infoOutputStream=null;
				}
			} catch (Exception e2) {
				// TODO: handle exception
				e2.printStackTrace();
			}
		}*/
        // Write serialized basic information of the tracklet to HDFS.

        return gsonBuilder.create().toJson(tracklet);
    }

    /**
     * Retrieve a tracklet from the HDFS.
     * Since a tracklet might be deleted from HDFS during reading,
     * it is highly recommended to retry this function on failure,
     * and the next time it will find the tracklet from HAR.
     *
     * @param storeDir the directory storing the tracklet (including only data of this tracklet).
     * @return the track retrieved.
     * @throws IOException        on failure of retrieving the tracklet.
     * @throws URISyntaxException on syntax error detected in the storeDir.
     */
    @Nonnull
    public static Tracklet retrieveTrackletNew(@Nonnull String storeDir,
                                               @Nonnull FileSystem hdfs) throws IOException, URISyntaxException {
        InputStreamReader infoReader = new InputStreamReader(hdfs.open(new Path(storeDir + "/info.txt")));
        FSDataInputStream imgInStream = hdfs.open(new Path(storeDir + "/tracklet.data"));
        InputStreamReader inputStreamReader=new InputStreamReader(imgInStream);
        BufferedReader br = new BufferedReader(inputStreamReader);

        // Read verbal informations of the track.
        Gson gson = new Gson();
        Tracklet tracklet = gson.fromJson(infoReader, Tracklet.class);

        // Read frames ...
        String jsonData = null;
        Map<Integer,String> trackletMap = new HashMap<Integer,String>();
        JsonParser jParser = new JsonParser(); 
        while ((jsonData = br.readLine()) != null) {
            // TODO
            JsonObject jObject = jParser.parse(jsonData).getAsJsonObject();
            String idxString = jObject.get("idx").getAsString();
            int idx = Integer.parseInt(idxString);
            String dataBase64String = jObject.get("data").getAsString();
            trackletMap.put(idx, dataBase64String);
        }
        if (br!=null) {
			
        	br.close(); 
        	br=null;
		}if (inputStreamReader!=null) {
			
			inputStreamReader.close(); 
			inputStreamReader=null;
		}if (imgInStream!=null) {
			
			imgInStream.close();
			imgInStream=null;
		}if (infoReader!=null) {
			
			infoReader.close();
			infoReader=null;
		}
		/*if (hdfs!=null) {
			hdfs.close();
			hdfs=null;
		}*/
        // Parse Data.
        ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
                .parallelStream()
                .forEach(idx -> {
                    Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];
                    boolean isSample = trackletMap.containsKey(idx);
                    if (isSample) {
                        String value = trackletMap.get(idx);
                        //JsonObject jObject = jParser.parse(value).getAsJsonObject();
                        //String dataBase64String = jObject.get("data").getAsString();
                        //int w = Integer.parseInt(jObject.get("width").getAsString());
                        //int h = Integer.parseInt(jObject.get("height").getAsString());
                        //int c = 3;
                        bbox.patchData = Base64.decodeBase64(value);
                    }
                });
        jsonData=null;
        trackletMap.clear();
        return tracklet;
    }
    
    /**
     * Retrieve a tracklet from the HDFS.
     * Since a tracklet might be deleted from HDFS during reading,
     * it is highly recommended to retry this function on failure,
     * and the next time it will find the tracklet from HAR.
     *
     * @param storeDir the directory storing the tracklet (including only data of this tracklet).
     * @return the track retrieved.
     * @throws IOException        on failure of retrieving the tracklet.
     * @throws URISyntaxException on syntax error detected in the storeDir.
     */
    @Nonnull
    public static Tracklet retrieveTrackletNew(@Nonnull String storeDir) throws Exception {
//    	getFileSystem();
        return retrieveTrackletNew(storeDir, fsSingleton.getInst());
    }
    
    public static List<String> getTrackletUrl(String videoName){
    	String url="/user/vpetest.cripac/source_data/result/2018-05-09-10-27-57"+"/"+videoName;
    	System.out.println("url是："+url);
    	List<String> dirsAllList=new ArrayList<>();
    	Path s_path = new Path(url);
    	getFileSystem();
    	try {
			traverseFolder1(s_path,hdfs,dirsAllList);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return dirsAllList;
    }
    
    public static void traverseFolder1(Path path,FileSystem hdfs,List<String> dirsAllList) throws Exception{
		int eachFileNum = 0, eachDirNum = 0;
		if (hdfs.exists(path)) {
//			List<String> filesEachList = new ArrayList<String>();
			FileStatus[] fs =hdfs.listStatus(path);
       	 	Path[] paths = FileUtil.stat2Paths(fs);
       	 	for (int i = 0; i < paths.length; i++) {
       		
				if (hdfs.isDirectory(paths[i])) {
//					System.out.println("文件夹:" + paths[i].toString());
//					dirsAllList.add(paths[i].toString());
//					traverseFolder1(paths[i],hdfs,dirsAllList);
//					eachDirNum++;
					Path[] pathsIn =FileUtil.stat2Paths(hdfs.listStatus(paths[i]));
					for (int j = 0; j < pathsIn.length; j++) {
						if (hdfs.isDirectory(pathsIn[j])) {
							System.out.println("文件夹:" + pathsIn[j].toString());
							dirsAllList.add(pathsIn[j].toString());
							eachDirNum++;
						}
					}
				} 
			}
       	 	System.out.println("文件夹共有:" + eachDirNum + ",文件共有:" + eachFileNum);
       	 	
		}else {
			System.out.println("路径不存在");
		}

	}
    /**
     * Generate the image by a line in file named tracklet.data.
     *
     * @param videoURL         a property in the person node.
     * @param trackletDataPath path of tracklet.data in local filesystem.
     * @param outputDir        the dir that you want the image output to local
     *                         filesystem.
     *
     * @return the image filename generate by the frame data.
     */
    public static String[] generateImage(@Nonnull String videoURL,
                                       @Nonnull String trackletDataPath,
                                       @Nonnull String outputDir) throws Exception {
//    	FileOperation fo = new FileOperation();
//    	fo.createDirectory(outputDir);
//    	
//    	fo=null;
        String imageFilenameTemp = outputDir + "/" + videoURL + "-";
        // Load file ...
        String trackletDataFilename = trackletDataPath + "/tracklet.data";
        final InputStreamReader dataReader = 
            new InputStreamReader(new FileInputStream(new File(trackletDataFilename)));
        BufferedReader br = new BufferedReader(dataReader);
        // Read line by line.
        JsonParser jParser = new JsonParser();
        String jsonData = null;
        List<String> imgFilenames = new ArrayList<String>();
        while ((jsonData = br.readLine()) != null) {
            JsonObject jObject = jParser.parse(jsonData).getAsJsonObject();
            int w = Integer.parseInt(jObject.get("width").getAsString());
            int h = Integer.parseInt(jObject.get("height").getAsString());
            int idx = Integer.parseInt(jObject.get("idx").getAsString());
            String dataBase64String = jObject.get("data").getAsString();
            if(0==w*h){
            	continue;
            }
            byte[] data = Base64.decodeBase64(dataBase64String);
            // Generate images.
            final BytePointer inputPointer = new BytePointer(data);
            final opencv_core.Mat image = new opencv_core.Mat(h, w, CV_8UC3, inputPointer);
            final BytePointer outputPointer = new BytePointer();
            imencode(".jpg", image, outputPointer);
            // Output image.
            String filename = imageFilenameTemp + idx + ".jpg";
            imgFilenames.add(filename);
            final byte[] bytes = new byte[(int) outputPointer.limit()];
            outputPointer.get(bytes);
            final FileOutputStream imgOutputStream;
            try {
                imgOutputStream = new FileOutputStream(new File(filename));
                imgOutputStream.write(bytes);
                imgOutputStream.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
            // Free resources.
            image.release();
            inputPointer.deallocate();
            outputPointer.deallocate();
        }
        br.close();

        String[] names = new String[imgFilenames.size()];
        imgFilenames.toArray(names);
        return names;
    }

    public static void main(String[] args) {
    	List<String> dirsAllList=getTrackletUrl("CAM01-20131223120147-20131223120735");
    	for (int i = 0; i < dirsAllList.size(); i++) {
			System.out.println(dirsAllList.get(i));
		}
	}
}
