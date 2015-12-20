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

package msc.fall2015.stock.kmeans.hbase.mapreduce;

import msc.fall2015.stock.kmeans.hbase.utils.TableUtils;
import msc.fall2015.stock.kmeans.hbase.utils.VectorPoint;
import msc.fall2015.stock.kmeans.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class StockVectorCalculator {
    private static String startDate;
    private static String endDate;
    private static final Logger log = LoggerFactory.getLogger(StockDataReader.class);

    public static void main(String[] args) {
        try {
            startDate = args[1];
            endDate = args[2];
            System.out.println("Start Date : " + startDate);
            System.out.println("End Date : " + endDate);
            if (startDate == null || startDate.isEmpty()) {
                // set 1st starting date
                startDate = "20040102";
            }
            if (endDate == null || endDate.isEmpty()) {
                endDate = "20141231";
            }
            Configuration config = HBaseConfiguration.create();
            Job job = new Job(config,"ExampleSummaryToFile");
            job.setJarByClass(StockDataReaderMapper.class);     // class that contains mapper

            Scan scan = new Scan();
            scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);  // don't set to true for MR jobs
            List<String> suitableDates = TableUtils.getDates(startDate, endDate);
            if (suitableDates != null && !suitableDates.isEmpty()){
                System.out.println("******* Date Count : " + suitableDates.size());
                for (String date : suitableDates){
                    scan.addColumn(Constants.STOCK_TABLE_CF_BYTES, date.getBytes());
                }
            }
            TableMapReduceUtil.initTableMapperJob(
                    Constants.STOCK_TABLE_NAME,        // input HBase table name
                    scan,             // Scan instance to control CF and attribute selection
                    StockDistanceCalculatorMapper.class,   // mapper
                    Text.class,             // mapper output key
                    VectorPoint.class,             // mapper output value
                    job);
            FileOutputFormat.setOutputPath(job, new Path(Constants.HDFS_OUTPUT_PATH + "vector_" + startDate + "_" + endDate));  // adjust directories as required
            boolean b = job.waitForCompletion(true);
            if (!b) {
                throw new IOException("error with job!");
            }
        } catch (ParseException e) {
            log.error("Error while parsing date", e);
            throw new RuntimeException("Error while parsing date", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}