package msc.fall2015.stock.kmeans.hbase.mapreduce.pwd;

public class Constants {
	public static String BLOCK_SIZE = "BLOCK SIZE";

	public static String NO_OF_DIVISIONS = "#divisions";
	
	public static String NO_OF_SEQUENCES = "#sequences";
	public static String WEIGHT_ENABLED = "weightEnabled";

	public static String HDFS_SEQ_FILENAME = "inputSequencePart";
	
	public static String BREAK =  "#";
	
	static enum RecordCounters { ALIGNMENTS, REDUCES };
}
