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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Thilina Gunarathne (tgunarat@cs.indiana.edu)
 */

public class PairWiseAlignment extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PairWiseAlignment(),
				args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err
					.println("Usage:  <sequence_file> <sequence_count> <block_size> <weight>");
			System.exit(2);
		}

		/* input parameters */
		String sequenceFile = args[1];
        System.out.println(sequenceFile);
        // we are limited to int's as java loops supports only them
		int noOfSequences = Integer.parseInt(args[2]);
//		int noOfSequences = 7322;
		int blockSize = Integer.parseInt(args[3]);

        boolean weightCalculate = Boolean.parseBoolean(args[4]);
//		int blockSize = 7322;

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Pairwise-analysis");

		/* create the base dir for this job. Delete and recreates if it exists */
		Path hdMainDir = new Path(msc.fall2015.stock.kmeans.utils.Constants.HDFS_HOME_PATH + "swg-hadoop");
        FileSystem fs = FileSystem.get(conf);
		fs.delete(hdMainDir, true);
		Path hdInputDir = new Path(hdMainDir, "data");
		if (!fs.mkdirs(hdInputDir)) {
			throw new IOException("Mkdirs failed to create"
					+ "/swg-hadoop/data");
		}

		int noOfDivisions = (int) Math.ceil(noOfSequences / (double) blockSize);
		int noOfBlocks = (noOfDivisions * (noOfDivisions + 1)) / 2;
		System.out.println("No of divisions :" + noOfDivisions
				+ "\nNo of blocks :" + noOfBlocks + "\nBlock size :"
				+ blockSize);

		// Retrieving the configuration form the job to set the properties
		// Setting properties to the original conf does not work (possible
		// Hadoop bug)
		Configuration jobConf = job.getConfiguration();

		// Input dir in HDFS. Create this in newly created job base dir
		Path inputDir = new Path(hdMainDir, "input");
		if (!fs.mkdirs(inputDir)) {
			throw new IOException("Mkdirs failed to create "
					+ inputDir.toString());
		}

		Long dataPartitionStartTime = System.nanoTime();
		partitionData(sequenceFile, noOfSequences, blockSize, fs,
				noOfDivisions, jobConf, inputDir);

		distributeData(blockSize, conf, fs, hdInputDir, noOfDivisions);

		long dataPartTime = (System.nanoTime() - dataPartitionStartTime) / 1000000;
		System.out.println("Data Partition & Scatter Completed in (ms):"
				+ dataPartTime);

		// Output dir in HDFS
		Path hdOutDir = new Path(hdMainDir, "out");

		jobConf.setInt(Constants.BLOCK_SIZE, blockSize);
		jobConf.setInt(Constants.NO_OF_DIVISIONS, noOfDivisions);
		jobConf.setInt(Constants.NO_OF_SEQUENCES, noOfSequences);
        jobConf.setBoolean(Constants.WEIGHT_ENABLED, weightCalculate);

		job.setJarByClass(PairWiseAlignment.class);
		job.setMapperClass(SWGMap.class);
		job.setReducerClass(SWGReduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(SWGWritable.class);
		FileInputFormat.setInputPaths(job, hdInputDir);
		FileOutputFormat.setOutputPath(job, hdOutDir);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks((int) noOfDivisions);

		long startTime = System.currentTimeMillis();
		int exitStatus = job.waitForCompletion(true) ? 0 : 1;
		double executionTime = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Job Finished in " + executionTime + " seconds");
		
		if (args.length == 5) {
			FileWriter writer = new FileWriter(args[4]);
			writer.write("# #seq\t#blockS\tTtime\tinput\tdataDistTime\toutput");
			writer.write("\n");
			writer.write(noOfSequences + "\t" + noOfBlocks + "\t"
					+ executionTime + "\t" + sequenceFile + "\t" + dataPartTime
					+ "\t" + hdMainDir);
			writer.write("\n");
			writer.flush();
			writer.close();
		}
		return exitStatus;
	}

	private void distributeData(int blockSize, Configuration conf,
			FileSystem fs, Path hdInputDir, int noOfDivisions)
			throws IOException {
		// Writing block meta data to for each block in a separate file so that
		// Hadoop will create separate Map tasks for each block..
		// Key : block number
		// Value: row#column#isDiagonal#base_file_name
		// TODO : find a better way to do this.
		for (int row = 0; row < noOfDivisions; row++) {
			for (int column = 0; column < noOfDivisions; column++) {
				// using the load balancing algorithm to select the blocks
				// include the diagonal blocks as they are blocks, not
				// individual pairs
				if (((row >= column) & ((row + column) % 2 == 0))
						| ((row <= column) & ((row + column) % 2 == 1))) {
					Path vFile = new Path(hdInputDir, "data_file_" + row + "_"
							+ column);
					SequenceFile.Writer vWriter = SequenceFile.createWriter(fs,
							conf, vFile, LongWritable.class, Text.class,
							CompressionType.NONE);

					boolean isDiagonal = false;
					if (row == column) {
						isDiagonal = true;
					}
					String value = row + Constants.BREAK + column
							+ Constants.BREAK + isDiagonal + Constants.BREAK
							+ Constants.HDFS_SEQ_FILENAME;
					vWriter.append(new LongWritable(row * blockSize + column),
							new Text(value));
					vWriter.close();
				}
			}
		}
	}

	private void partitionData(String sequenceFile, int noOfSequences,
			int blockSize, FileSystem fs, int noOfDivisions,
			Configuration jobConf, Path inputDir) throws FileNotFoundException,
			IOException, URISyntaxException {
		// Break the sequences file in to parts based on the block size. Stores
		// the parts in HDFS and add them to the Hadoop distributed cache.
        Path path = new Path(sequenceFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));

        System.out.println("noOfDivisions : " + noOfDivisions);
        System.out.println("blockSize : " + blockSize);
		for (int partNo = 0; partNo < noOfDivisions; partNo++) {
			//
			String filePartName = Constants.HDFS_SEQ_FILENAME + "_" + partNo;
			Path inputFilePart = new Path(inputDir, filePartName);
			OutputStream partOutStream = fs.create(inputFilePart);
			BufferedWriter bufferedWriter = new BufferedWriter(
					new OutputStreamWriter(partOutStream));

			for (int sequenceIndex = 0; ((sequenceIndex < blockSize) & (sequenceIndex
					+ (partNo * blockSize) < noOfSequences)); sequenceIndex++) {
				String line;
                line = bufferedReader.readLine();
                if (line == null) {
					throw new IOException(
							"Cannot read the sequence from input file.");
				}
				// write the sequence name
				bufferedWriter.write(line);
				bufferedWriter.newLine();
			}
			bufferedWriter.flush();
			bufferedWriter.close();
			// Adding the sequences file to Hadoop cache
			URI cFileURI = new URI(inputFilePart.toUri() + "#" + filePartName);
			DistributedCache.addCacheFile(cFileURI, jobConf);
			DistributedCache.createSymlink(jobConf);
		}
	}
}