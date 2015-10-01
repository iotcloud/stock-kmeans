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

import msc.fall2015.stock.kmeans.hbase.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class HBaseDataInserter {
    private static String STOCK_TABLE_NAME = "Stock2004_2014Table";
    private static String STOCK_TABLE_CF = "Stock2004_2014CF";
    private static String ID_COLUMN = "id";
    private static String DATE_COLUMN = "date";
    private static String SYMBOL_COLUMN = "symbol";
    private static String PRICE_COLUMN = "price";
    private static String CAP_COLUMN = "cap";
    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
//            configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
            configuration.clear();
            configuration.set("hbase.zookeeper.quorum", "156.56.179.122");
            configuration.set("hbase.zookeeper.property.clientPort","2181");
            configuration.set("hbase.master", "156.56.179.122:16010");
            configuration.set("hbase.rootdir", "hdfs://156.56.179.122:9000/hbase");
            HBaseAdmin.checkHBaseAvailable(configuration);
            Connection connection = ConnectionFactory.createConnection(configuration);



            System.out.println("HBase is running!");
//            deleteTable(connection, STOCK_TABLE_NAME);
//            createTable(connection);
            listTableContent(connection);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static void deleteTable (Connection connection, String tableName){
        try {
            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);
            admin.disableTable(table);
            admin.deleteTable(table);
            System.out.println("Table " + tableName + " is deleted...");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void test (){
        String line = "10002,20130219,BTFG,,";
        String[] split = line.split(",");
        if (split.length > 0 && split[0] != null && !split[0].isEmpty()){
            System.out.println(split[0]);
        }

        if (split.length > 1 && split[1] != null && !split[1].isEmpty()){
            System.out.println(split[1]);
        }

        if (split.length > 2 && split[2] != null && !split[2].isEmpty()) {
            System.out.println(split[2]);
        }

        if (split.length > 3 && split[3] != null && !split[3].isEmpty()) {
            System.out.println(split[3]);
        }

        if (split.length > 4 && split[4] != null && !split[4].isEmpty()) {
            System.out.println(split[4]);
        }

    }

    public static void createTable(Connection connection){
        String csvFile = "/Users/chathuri/MSc/summer_2015/source/stock/stock-kmeans/src/main/resources/2004_2014.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        try{
            // Instantiating HbaseAdmin class
            Admin admin = connection.getAdmin();

            // Instantiating table descriptor class
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(STOCK_TABLE_NAME));

            // Adding column families to table descriptor
            HColumnDescriptor stock_0414 = new HColumnDescriptor(STOCK_TABLE_CF);
            tableDescriptor.addFamily(stock_0414);
//            tableDescriptor.addFamily(new HColumnDescriptor("professional"));

            // Execute the table through admin
            if (!admin.tableExists(tableDescriptor.getTableName())){
                admin.createTable(tableDescriptor);
                System.out.println("table created !!!");
            }

            Table table = connection.getTable(tableDescriptor.getTableName());
            byte[] idCol = Bytes.toBytes(ID_COLUMN);
            byte[] dateCol = Bytes.toBytes(DATE_COLUMN);
            byte[] symbolCol = Bytes.toBytes(SYMBOL_COLUMN);
            byte[] priceCol = Bytes.toBytes(PRICE_COLUMN);
            byte[] capCol = Bytes.toBytes(CAP_COLUMN);

            // read CRV file

            br = new BufferedReader(new FileReader(csvFile));
            int index = 1;
            while ((line = br.readLine()) != null) {
//                System.out.println(index);
                Put row = new Put(Bytes.toBytes(index));
                // use comma as separator
                String[] stockData = line.split(cvsSplitBy);
                // adding values using add() method
                // accepts column family name, qualifier/row name ,value
                if (stockData.length > 0 && stockData[0] != null && !stockData[0].isEmpty()){
                    row.add(stock_0414.getName(),
                            idCol, Bytes.toBytes(stockData[0]));
                }

                if (stockData.length > 1 && stockData[1] != null && !stockData[1].isEmpty()){
                    row.add(stock_0414.getName(),
                            dateCol, Bytes.toBytes(stockData[1]));
                }

                if (stockData.length > 2 && stockData[2] != null && !stockData[2].isEmpty()) {
                    row.add(stock_0414.getName(),
                            symbolCol, Bytes.toBytes(stockData[2]));
                }

                if (stockData.length > 3 && stockData[3] != null && !stockData[3].isEmpty()) {
                    row.add(stock_0414.getName(),
                            priceCol, Bytes.toBytes(stockData[3]));
                }

                if (stockData.length > 4 && stockData[4] != null && !stockData[4].isEmpty()) {
                    row.add(stock_0414.getName(),
                            capCol, Bytes.toBytes(stockData[4]));
                }

                // Saving the put Instance to the HTable.
                table.put(row);
                index++;
            }

            System.out.println("data inserted");
            // closing HTable
            table.close();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void listTableContent(Connection connection){
        try {
            Admin admin = connection.getAdmin();

            // Getting all the list of tables using HBaseAdmin object
            HTableDescriptor[] tableDescriptor = admin.listTables();

            // printing all the table names.
            for (int i=0; i<tableDescriptor.length;i++ ){
                if (tableDescriptor[i].getTableName().getNameAsString().equals(Constants.STOCK_TABLE_NAME)){
                    Table table = connection.getTable(tableDescriptor[i].getTableName());
                    Scan scan = new Scan();
                    scan.setCaching(20);
                    scan.addFamily(Constants.STOCK_TABLE_CF_BYTES);
                    ResultScanner scanner = table.getScanner(scan);
                    printRows(scanner);
                }
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printRows(ResultScanner resultScanner) {
        for (Iterator<Result> iterator = resultScanner.iterator(); iterator.hasNext();) {
            printRow(iterator.next());
        }
    }
    public static void printRow(Result result) {
        String returnString = "";
        returnString += Bytes.toString(result.getValue(Constants.STOCK_TABLE_CF_BYTES, Constants.ID_COLUMN_BYTES)) + ", ";
        returnString += Bytes.toString(result.getValue(Constants.STOCK_TABLE_CF_BYTES, Constants.DATE_COLUMN_BYTES)) + ", ";
        returnString += Bytes.toString(result.getValue(Constants.STOCK_TABLE_CF_BYTES, Constants.SYMBOL_COLUMN_BYTES)) + ", ";
        returnString += Bytes.toString(result.getValue(Constants.STOCK_TABLE_CF_BYTES, Constants.PRICE_COLUMN_BYTES)) + ", ";
        returnString += Bytes.toString(result.getValue(Constants.STOCK_TABLE_CF_BYTES, Constants.CAP_COLUMN_BYTES));
        System.out.println(returnString);
    }

}
