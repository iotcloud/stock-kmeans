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

import com.google.protobuf.ServiceException;
import msc.fall2015.stock.kmeans.hbase.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseBulkDataLoader {
    private static final Logger log = LoggerFactory.getLogger(HBaseBulkDataLoader.class);

    public static void main(String[] args) {
        try {
            Configuration configuration =  HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "156.56.179.122");
            configuration.set("hbase.zookeeper.property.clientPort","2181");
            configuration.set("hbase.master", "156.56.179.122:16010");
            configuration.set("hbase.rootdir","hdfs://156.56.179.122:9000/hbase");
            HBaseAdmin.checkHBaseAvailable(configuration);
            Connection connection = ConnectionFactory.createConnection(configuration);

            // Instantiating HbaseAdmin class
            Admin admin = connection.getAdmin();

            // Instantiating table descriptor class
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Constants.STOCK_TABLE_NAME));

            // Adding column families to table descriptor
            HColumnDescriptor stock_0414 = new HColumnDescriptor(Constants.STOCK_TABLE_CF);
            tableDescriptor.addFamily(stock_0414);
//            tableDescriptor.addFamily(new HColumnDescriptor("professional"));

            // Execute the table through admin
            if (!admin.tableExists(tableDescriptor.getTableName())){
                admin.createTable(tableDescriptor);
                System.out.println("table created !!!");
            }
            // Load hbase-site.xml
            HBaseConfiguration.addHbaseResources(configuration);
            Job job = configureJob(configuration);
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        } catch (ServiceException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    public static Job configureJob(Configuration configuration) throws IOException {
        Job job = new Job(configuration, "HBase Bulk Import Example");
        job.setJarByClass(HBaseKVMapper.class);

        job.setMapperClass(HBaseKVMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(Constants.HBASE_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Constants.HBASE_OUTPUT_PATH));
        TableMapReduceUtil.initTableReducerJob(Constants.STOCK_TABLE_NAME, null, job);
        job.setNumReduceTasks(0);
        return job;
    }
}
