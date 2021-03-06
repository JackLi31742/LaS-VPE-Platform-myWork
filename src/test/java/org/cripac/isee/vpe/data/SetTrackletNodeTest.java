/**
 * To test the function getTrackletInfo in HadoopHelper
 * 
 * @Author da.li on 2017-04-17
 */

package org.cripac.isee.vpe.data;

import org.cripac.isee.vpe.data.Neo4jConnector;
import org.cripac.isee.vpe.data.GraphDatabaseConnector;

public class SetTrackletNodeTest {

    public static void main(String[] args) throws Exception {
     
        // It is necessary to change the directory as your practical situation.
        String storeDir = "/user/labadmin/metadata/20131223102739-20131223103331/280f5b30-1149-4dfa-9462-12fd8f307edc.har/9";

        GraphDatabaseConnector dbConnector = new Neo4jConnector();
        long startTime = System.currentTimeMillis();
        dbConnector.setTrackletSavingPath("test_1a", storeDir);
        long endTime = System.currentTimeMillis();
        System.out.println("Cost time of insert a node: " + (endTime - startTime) + "ms");
    }
}
