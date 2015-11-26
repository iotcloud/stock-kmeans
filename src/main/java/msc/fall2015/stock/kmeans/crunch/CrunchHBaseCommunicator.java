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

package msc.fall2015.stock.kmeans.crunch;

import msc.fall2015.stock.kmeans.hbase.utils.Constants;
import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;

public class CrunchHBaseCommunicator extends Configured implements Tool, Serializable {
    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run(new Configuration(), new CrunchHBaseCommunicator(), args);
        System.exit(res);
    }

    @Override
    public int run(final String[] args) throws Exception {

        final Configuration config = getConf();
        final Pipeline pipeline = new MRPipeline(CrunchHBaseCommunicator.class,
                "PipelineWithFilterFn", config);
        PCollection<String> lines = pipeline.readTextFile(Constants.HBASE_INPUT_PATH);
        PCollection<String> splitLines = splitLines(lines);
        splitLines.count().write(To.textFile(Constants.HBASE_OUTPUT_PATH + "crunch_output"));
        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

    public static PCollection<String> splitLines(PCollection<String> lines) {
        // This will work fine because the DoFn is defined inside of a static method.
        return lines.parallelDo(new DoFn<String, String>() {
            @Override
            public void process(String line, Emitter<String> emitter) {
                for (String word : line.split(",")) {
                    emitter.emit(word);
                }
            }
        }, Writables.strings());
    }
}
