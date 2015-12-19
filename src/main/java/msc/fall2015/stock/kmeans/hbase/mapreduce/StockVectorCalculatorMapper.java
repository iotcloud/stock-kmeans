package msc.fall2015.stock.kmeans.hbase.mapreduce;

import msc.fall2015.stock.kmeans.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class StockVectorCalculatorMapper extends TableMapper<Text, Text> {
    /**
     * The start date we are interested in
     */
    private String startDate;
    /**
     * The end  date we are interested in
     */
    private String endDate;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        startDate = conf.get(Constants.Job.START_DATE);
        endDate = conf.get(Constants.Job.END_DATE);
    }

    /**
     * Read the required columns and write to HDFS
     * @param row
     * @param value
     * @param context
     * @throws InterruptedException
     * @throws IOException
     */
    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
        // go through the column family
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyMap : value.getMap().entrySet()) {
            // go through the column
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entryVersion : columnFamilyMap.getValue().entrySet()) {
                // read all the values

            }
        }
    }
}
