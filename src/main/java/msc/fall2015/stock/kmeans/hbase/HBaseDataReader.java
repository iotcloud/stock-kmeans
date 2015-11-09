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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDataReader {
    private static final Logger log = LoggerFactory.getLogger(HBaseDataReader.class);
    private static List<String> allDates = new ArrayList<String>();

    public static void main(String[] args) {
        try {
            Configuration configuration =  HBaseConfiguration.create();
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
                    ResultScanner scanner = table.getScanner(scan);
                    allDates = getAllDates(scanner);
                }
            }
            for (HTableDescriptor aTableDescriptor : tableDescriptor) {
                if (aTableDescriptor.getTableName().getNameAsString().equals(Constants.STOCK_TABLE_NAME)) {
                    Table table = connection.getTable(aTableDescriptor.getTableName());
                    Scan scan = new Scan();
                    scan.setCaching(20);
                    scan.addFamily(Constants.STOCK_TABLE_CF_BYTES);
                    ResultScanner scanner = table.getScanner(scan);
                    printRows(scanner);
                }
            }
        }catch (IOException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        } catch (ServiceException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    public static List<String> getAllDates(ResultScanner scanner){
        List<String> dates = new ArrayList<String>();
        for (Result aResultScanner : scanner) {
            String date = new String(aResultScanner.getRow());
            dates.add(date);
        }
        return dates;
    }


    public static void printRows(ResultScanner resultScanner) {
        for (Result aResultScanner : resultScanner) {
            printRow(aResultScanner);
        }
    }
    public static void printRow(Result result) {
        try {
            String rowName = Bytes.toString(result.getRow());
            //if you want to get the entire row
            for (String date : allDates){
                byte[] value = result.getValue(Constants.STOCK_TABLE_CF_BYTES, date.getBytes());
                if (value != null){
                    System.out.println("Row Name : " + rowName + " : values : " + new String(value) );
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
