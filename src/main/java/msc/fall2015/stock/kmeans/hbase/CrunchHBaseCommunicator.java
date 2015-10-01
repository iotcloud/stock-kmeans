/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

package msc.fall2015.stock.kmeans.hbase;

import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.Serializable;

public class CrunchHBaseCommunicator extends Configured implements Tool, Serializable {
    public static void main(String[] args) {
        Pipeline pipeline = new MRPipeline(CrunchHBaseCommunicator.class);
//        pipeline.

    }

    public int run(String[] strings) throws Exception {
        return 0;
    }

    public static void stockPreprocessing (){
        // 1. breaking files
        // 2. generate vector files
        // 3. generate global vector file
        // 4. calculate the distance matrix for normal data
        // 5. calculate the distance matrix for global data set
        // 6. calculate the weigh matrix for yearly
        // 7. calculate the simple weigh file for yearly
        // 8. calculate the weight matrix for global
        // 9. calculate the simple weight file for global
    }
}
