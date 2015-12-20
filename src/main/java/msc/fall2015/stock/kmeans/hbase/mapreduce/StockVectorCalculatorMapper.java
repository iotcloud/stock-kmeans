package msc.fall2015.stock.kmeans.hbase.mapreduce;

import msc.fall2015.stock.kmeans.hbase.utils.CleanMetric;
import msc.fall2015.stock.kmeans.hbase.utils.VectorPoint;
import msc.fall2015.stock.kmeans.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class StockVectorCalculatorMapper extends TableMapper<Text, VectorPoint> {
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
            List<Double> priceList = new ArrayList<Double>();
            int count = 1;
            // go through the column
            double totalCap = 0;
            String rowKey = Bytes.toString(value.getRow());
            String[] idKey = rowKey.split("_");
            int id = Integer.valueOf(idKey[0]);
            String symbol = idKey[1];
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entryVersion : columnFamilyMap.getValue().entrySet()) {
                for (Map.Entry<Long, byte[]> entry : entryVersion.getValue().entrySet()) {
                    String column = Bytes.toString(entryVersion.getKey());
                    byte[] val = entry.getValue();
                    String valOfColumn = new String(val);
                    System.out.println("RowKey : " + rowKey + " Column Key : " + column + " Column Val : " + valOfColumn);
                    if (!valOfColumn.isEmpty()) {
                        String[] priceAndCap = valOfColumn.split("_");
                        if (priceAndCap.length > 1) {
                            String pr = priceAndCap[0];
                            String cap = priceAndCap[1];
                            if (pr != null && !pr.equals("null")){
                                double price = Double.valueOf(pr);
                                priceList.add(price);
                                System.out.println("Price : " + price + " count : " + count);
                            }
                            if (cap != null && !cap.equals("null")){
                                totalCap += Double.valueOf(cap);
                            }
                        }
                    }
                }
                count++;
            }
            double medianCap = totalCap / count;
            double[] priceArr = new double[priceList.size()];
            for (int i=0; i < priceList.size(); i++){
                priceArr[i] = priceList.get(i);
            }
            VectorPoint vectorPoint = new VectorPoint(id,symbol, priceArr,totalCap);
            vectorPoint.setElements(count);
            if(vectorPoint.cleanVector(new CleanMetric())){
                String serialize = vectorPoint.serialize();
                System.out.println(serialize);
            }
            context.write(new Text(String.valueOf(vectorPoint.getKey())), vectorPoint);
        }
    }
}
