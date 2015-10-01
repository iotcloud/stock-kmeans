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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class StockInsertPerYearMapper extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private static final Logger log = LoggerFactory.getLogger(HBaseBulkDataLoader.class);
    String tableName = "";

    /** {@inheritDoc} */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Configuration c = context.getConfiguration();

        tableName = c.get(Constants.STOCK_TABLE_NAME);
    }

    /** {@inheritDoc} */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = null;
        String date;
        long counterVal = context.getCounter("HBaseKVMapper", "NUM_MSGS").getValue();
        System.out.println(counterVal);
        try {
            fields = value.toString().split(",");
        } catch (Exception ex) {
            context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
            return;
        }

        date = fields[1];
        if (date != null && !date.isEmpty()){
            byte[] columnFamily;
            if (date.startsWith("2004")){
                columnFamily = Constants.STOCK_2004_CF_BYTES;
            }else if (date.startsWith("2005")){
                columnFamily = Constants.STOCK_2005_CF_BYTES;
            }else if (date.startsWith("2006")){
                columnFamily = Constants.STOCK_2006_CF_BYTES;
            }else if (date.startsWith("2007")){
                columnFamily = Constants.STOCK_2007_CF_BYTES;
            }else if (date.startsWith("2008")){
                columnFamily = Constants.STOCK_2008_CF_BYTES;
            }else if (date.startsWith("2009")){
                columnFamily = Constants.STOCK_2009_CF_BYTES;
            }else if (date.startsWith("2010")){
                columnFamily = Constants.STOCK_2010_CF_BYTES;
            }else if (date.startsWith("2011")){
                columnFamily = Constants.STOCK_2011_CF_BYTES;
            }else if (date.startsWith("2012")){
                columnFamily = Constants.STOCK_2012_CF_BYTES;
            }else if (date.startsWith("2013")){
                columnFamily = Constants.STOCK_2013_CF_BYTES;
            }else if (date.startsWith("2014")){
                columnFamily = Constants.STOCK_2014_CF_BYTES;
            }else {
                context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
                return;
            }
            byte[] rowKey = Bytes.toBytes(counterVal);
            Put row = new Put(rowKey);
            if (fields.length > 0 && fields[0] != null && !fields[0].equals("")) {
                row.add(columnFamily,
                        Constants.ID_COLUMN_BYTES, Bytes.toBytes(fields[0]));
            }

            if (fields.length > 1 && fields[1] != null && !fields[1].equals("")) {
                row.add(columnFamily,
                        Constants.DATE_COLUMN_BYTES, Bytes.toBytes(fields[1]));
            }

            if (fields.length > 2 && fields[2] != null && !fields[2].equals("")) {
                row.add(columnFamily,
                        Constants.SYMBOL_COLUMN_BYTES, Bytes.toBytes(fields[2]));
            }

            if (fields.length > 3 && fields[3] != null && !fields[3].equals("")) {
                row.add(columnFamily,
                        Constants.PRICE_COLUMN_BYTES, Bytes.toBytes(fields[3]));
            }

            if (fields.length > 4 && fields[4] != null && !fields[4].equals("")) {
                row.add(columnFamily,
                        Constants.CAP_COLUMN_BYTES, Bytes.toBytes(fields[4]));
            }
            context.write(new ImmutableBytesWritable(rowKey), row);
            context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);
        }


    }
}
