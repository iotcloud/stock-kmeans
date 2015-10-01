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

package msc.fall2015.stock.kmeans.hbase.utils;

public class Constants {
    public static final String STOCK_TABLE_NAME = "Stock2004_2014Table";
    public static final String STOCK_TABLE_CF = "Stock2004_2014CF";
    public static final String STOCK_2004_CF = "Stock2004_CF";
    public static final String STOCK_2005_CF = "Stock2005_CF";
    public static final String STOCK_2006_CF = "Stock2006_CF";
    public static final String STOCK_2007_CF = "Stock2007_CF";
    public static final String STOCK_2008_CF = "Stock2008_CF";
    public static final String STOCK_2009_CF = "Stock2009_CF";
    public static final String STOCK_2010_CF = "Stock2010_CF";
    public static final String STOCK_2011_CF = "Stock2011_CF";
    public static final String STOCK_2012_CF = "Stock2012_CF";
    public static final String STOCK_2013_CF = "Stock2013_CF";
    public static final String STOCK_2014_CF = "Stock2014_CF";
    public static final String ID_COLUMN = "id";
    public static final String DATE_COLUMN = "date";
    public static final String SYMBOL_COLUMN = "symbol";
    public static final String PRICE_COLUMN = "price";
    public static final String CAP_COLUMN = "cap";

    public static final byte[] STOCK_TABLE_NAME_BYTES = STOCK_TABLE_NAME.getBytes();
    public static final byte[] STOCK_TABLE_CF_BYTES = STOCK_TABLE_CF.getBytes();
    public static final byte[] STOCK_2004_CF_BYTES = STOCK_2004_CF.getBytes();
    public static final byte[] STOCK_2005_CF_BYTES = STOCK_2005_CF.getBytes();
    public static final byte[] STOCK_2006_CF_BYTES = STOCK_2006_CF.getBytes();
    public static final byte[] STOCK_2007_CF_BYTES = STOCK_2007_CF.getBytes();
    public static final byte[] STOCK_2008_CF_BYTES = STOCK_2008_CF.getBytes();
    public static final byte[] STOCK_2009_CF_BYTES = STOCK_2009_CF.getBytes();
    public static final byte[] STOCK_2010_CF_BYTES = STOCK_2010_CF.getBytes();
    public static final byte[] STOCK_2011_CF_BYTES = STOCK_2011_CF.getBytes();
    public static final byte[] STOCK_2012_CF_BYTES = STOCK_2012_CF.getBytes();
    public static final byte[] STOCK_2013_CF_BYTES = STOCK_2013_CF.getBytes();
    public static final byte[] STOCK_2014_CF_BYTES = STOCK_2014_CF.getBytes();
    public static final byte[] ID_COLUMN_BYTES = ID_COLUMN.getBytes();
    public static final byte[] DATE_COLUMN_BYTES = DATE_COLUMN.getBytes();
    public static final byte[] SYMBOL_COLUMN_BYTES = SYMBOL_COLUMN.getBytes();
    public static final byte[] PRICE_COLUMN_BYTES = PRICE_COLUMN.getBytes();
    public static final byte[] CAP_COLUMN_BYTES = CAP_COLUMN.getBytes();

    public static final String HBASE_INPUT_PATH = "hdfs://156.56.179.122:9000/input";
    public static final String HBASE_OUTPUT_PATH = "hdfs://156.56.179.122:9000/output/";


}
