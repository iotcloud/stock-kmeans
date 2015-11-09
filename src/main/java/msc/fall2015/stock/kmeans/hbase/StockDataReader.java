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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class StockDataReader {
    private static String startDate;
    private static String endDate;
    private static final Logger log = LoggerFactory.getLogger(StockDataReader.class);

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
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
            Job job = new Job(config, "ExampleRead");
            job.setJarByClass(HBaseDataReaderMapper.class);     // class that contains mapper

            Scan scan = new Scan();
            scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);  // don't set to true for MR jobs
            List<String> suitableDates = getDates();
            if (suitableDates != null && !suitableDates.isEmpty()){
                System.out.println("******* Date Count : " + suitableDates.size());
                for (String date : suitableDates){
                    scan.addColumn(Constants.STOCK_TABLE_CF_BYTES, date.getBytes());
                }
            }
            TableMapReduceUtil.initTableMapperJob(
                    Constants.STOCK_TABLE_NAME,        // input HBase table name
                    scan,             // Scan instance to control CF and attribute selection
                    HBaseDataReaderMapper.class,   // mapper
                    null,             // mapper output key
                    null,             // mapper output value
                    job);
            job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper

            boolean b = job.waitForCompletion(true);
            if (!b) {
                throw new IOException("error with job!");
            }
        } catch (ParseException e) {
            log.error("Error while parsing date", e);
            throw new RuntimeException("Error while parsing date", e);
        }

    }

    public static List<String> getDates() throws ParseException {
        List<String> allDates = new ArrayList<String>();
        Date startDate = getDate(StockDataReader.startDate);
        Date endDate = getDate(StockDataReader.endDate);
        ResultScanner scannerForDateTable = getScannerForDateTable();
        for (Result aResultScanner : scannerForDateTable) {
            String date = new String(aResultScanner.getRow());
            Date rowDate = getDate(date);
            if (startDate.compareTo(rowDate) * rowDate.compareTo(endDate) > 0){
                System.out.println("***** DATE ******** : " + date);
                allDates.add(date);
            }
        }
        return allDates;

    }

    public static Date getDate (String date) throws ParseException {
        System.out.println(date);
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        Date parse = df.parse(date);
        System.out.println(parse);
        return parse;
    }

    private static ResultScanner getScannerForDateTable() {
        try {
            Configuration configuration = HBaseConfiguration.create();
            HBaseConfiguration.addHbaseResources(configuration);
            HBaseAdmin.checkHBaseAvailable(configuration);
            Connection connection = ConnectionFactory.createConnection(configuration);
            // Instantiating HbaseAdmin class
            Admin admin = connection.getAdmin();

            HTableDescriptor[] tableDescriptor = admin.listTables();
            // printing all the table names.
            for (HTableDescriptor aTableDescriptor : tableDescriptor) {
                if (aTableDescriptor.getTableName().getNameAsString().equals(Constants.STOCK_DATES_TABLE)) {
                    Table table = connection.getTable(aTableDescriptor.getTableName());
                    Scan scan = new Scan();
                    scan.setCaching(20);
                    scan.addFamily(Constants.STOCK_DATES_CF_BYTES);
                    return table.getScanner(scan);
                }
            }
        } catch (ServiceException e) {
            log.error("Error while reading Stock Dates table", e);
        } catch (MasterNotRunningException e) {
            log.error("Error while reading Stock Dates table", e);
        } catch (ZooKeeperConnectionException e) {
            log.error("Error while reading Stock Dates table", e);
        } catch (IOException e) {
            log.error("Error while reading Stock Dates table", e);
        }
        return null;
    }
}
