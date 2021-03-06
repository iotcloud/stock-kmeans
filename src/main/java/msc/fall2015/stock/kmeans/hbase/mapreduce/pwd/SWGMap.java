/**
 * Software License, Version 1.0
 * 
 * Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 * 
 *
 *Redistribution and use in source and binary forms, with or without 
 *modification, are permitted provided that the following conditions are met:
 *
 *1) All redistributions of source code must retain the above copyright notice,
 * the list of authors in the original source code, this list of conditions and
 * the disclaimer listed in this license;
 *2) All redistributions in binary form must reproduce the above copyright 
 * notice, this list of conditions and the disclaimer listed in this license in
 * the documentation and/or other materials provided with the distribution;
 *3) Any documentation included with all redistributions must include the 
 * following acknowledgement:
 *
 *"This product includes software developed by the Community Grids Lab. For 
 * further information contact the Community Grids Lab at 
 * http://communitygrids.iu.edu/."
 *
 * Alternatively, this acknowledgement may appear in the software itself, and 
 * wherever such third-party acknowledgments normally appear.
 * 
 *4) The name Indiana University or Community Grids Lab or NaradaBrokering, 
 * shall not be used to endorse or promote products derived from this software 
 * without prior written permission from Indiana University.  For written 
 * permission, please contact the Advanced Research and Technology Institute 
 * ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 *5) Products derived from this software may not be called NaradaBrokering, 
 * nor may Indiana University or Community Grids Lab or NaradaBrokering appear
 * in their name, without prior written permission of ARTI.
 * 
 *
 * Indiana University provides no reassurances that the source code provided 
 * does not infringe the patent or any other intellectual property rights of 
 * any other entity.  Indiana University disclaims any liability to any 
 * recipient for claims brought by any other entity based on infringement of 
 * intellectual property rights or otherwise.  
 *
 *LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO 
 *WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 *NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF 
 *INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS. 
 *INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS", 
 *"VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.  
 *LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR 
 *ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION 
 *GENERATED USING SOFTWARE.
 */

package msc.fall2015.stock.kmeans.hbase.mapreduce.pwd;

import java.io.IOException;
import java.util.List;

import msc.fall2015.stock.kmeans.hbase.utils.VectorPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Thilina Gunarathne (tgunarat@cs.indiana.edu)
 */

public class SWGMap extends Mapper<LongWritable, Text, LongWritable, SWGWritable> {

	public void map(LongWritable blockIndex, Text value, Context context)
			throws IOException, InterruptedException {
		long startTime = System.nanoTime();
		Configuration conf = context.getConfiguration();
		Counter alignmentCounter = context
				.getCounter(Constants.RecordCounters.ALIGNMENTS);
		String valString = value.toString();
		String valArgs[] = valString.split(Constants.BREAK);

		long rowBlock = Long.parseLong(valArgs[0]);
		long columnBlock = Long.parseLong(valArgs[1]);
		boolean isDiagonal = Boolean.parseBoolean(valArgs[2]);
		System.out.println("row column" + rowBlock + "  " + columnBlock + "  "
				+ isDiagonal + "  " + valArgs[2]);

		long blockSize = conf.getLong(Constants.BLOCK_SIZE, 1000);
		long noOfSequences = conf.getLong(Constants.NO_OF_SEQUENCES,
				blockSize * 10);
		long noOfDivisions = conf.getLong(Constants.NO_OF_DIVISIONS,
				noOfSequences / blockSize);
        boolean weightEnabled = conf.getBoolean(Constants.WEIGHT_ENABLED, false);

		long row = rowBlock * blockSize;
		long column = columnBlock * blockSize;

		long parseStartTime = System.nanoTime();
		FileSystem fs = FileSystem.getLocal(conf);
		// parse the inputFilePart for row
		Path rowPath = new Path(Constants.HDFS_SEQ_FILENAME + "_" + rowBlock);
		FSDataInputStream rowInStream = fs.open(rowPath);
		List<VectorPoint> rowSequences;
		rowSequences = SequenceParser.ParseFile(rowInStream);
		// parse the inputFilePart for column if this is not a diagonal block
		List<VectorPoint> colSequences;
		if (isDiagonal) {
			colSequences = rowSequences;
		} else {
			// parse the inputFilePart for column
			Path colPath = new Path(Constants.HDFS_SEQ_FILENAME + "_"
					+ columnBlock);
			FSDataInputStream colInStream = fs.open(colPath);
			colSequences = SequenceParser.ParseFile(colInStream);
		}
		System.out.println("Parsing time : "
				+ ((System.nanoTime() - parseStartTime) / 1000000) + "ms");

		short[][] alignments = new short[(int) blockSize][(int) blockSize];
		for (int rowIndex = 0; ((rowIndex < blockSize) & ((row + rowIndex) < noOfSequences)); rowIndex++) {
			int columnIndex = 0;
			for (; ((columnIndex < blockSize) & ((column + columnIndex) < noOfSequences)); columnIndex++) {
                double alignment = 0;
                if (weightEnabled){
                    alignment = rowSequences.get(rowIndex).weight(colSequences.get(columnIndex));
                }else {
                    alignment = rowSequences.get(rowIndex).corr(colSequences.get(columnIndex));
                }

                // Get the identity and make it percent identity
                short scaledScore = (short) (alignment * Short.MAX_VALUE);
                alignments[rowIndex][columnIndex] = scaledScore;
            }
			alignmentCounter.increment(columnIndex);
		}

		SWGWritable dataWritable = new SWGWritable(rowBlock, columnBlock,
				blockSize, false);
		dataWritable.setAlignments(alignments);
		context.write(new LongWritable(rowBlock), dataWritable);

		if (!isDiagonal) {
			// Create the transpose matrix of (rowBlock, colBlock) block to fill the
			// (colBlock, rowBlock) block.
			SWGWritable inverseDataWritable = new SWGWritable(columnBlock,
					rowBlock, blockSize, true);
			inverseDataWritable.setAlignments(alignments);
			context.write(new LongWritable(columnBlock), inverseDataWritable);
		}
		System.out.println("Map time : "
				+ ((System.nanoTime() - startTime) / 1000000) + "ms");
	}
}
