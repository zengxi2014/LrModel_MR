package com.uc.bigdata.exercise;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FeatureTask {
	/**
	 * 得到一个目录(不包括子目录)下的所有名字匹配上pattern的文件名
	 * 
	 * @param fs
	 * @param folderPath
	 * @param pattern
	 *            用于匹配文件名的正则
	 * @return
	 * @throws IOException
	 */
	public static List<Path> getFilesUnderFolder(FileSystem fs, Path folderPath, String pattern) throws IOException {
		List<Path> paths = new ArrayList<Path>();
		if (fs.exists(folderPath)) {
			FileStatus[] fileStatus = fs.listStatus(folderPath);
			for (int i = 0; i < fileStatus.length; i++) {
				FileStatus fileStatu = fileStatus[i];
				// 如果是目录，递归向下找
				Path oneFilePath = fileStatu.getPath();
				if (!fileStatu.isDir()) {

					if (pattern == null) {
						paths.add(oneFilePath);
					} else {
						if (oneFilePath.getName().contains(pattern)) {
							paths.add(oneFilePath);
						}
					}
				} else {
					paths.addAll(getFilesUnderFolder(fs, oneFilePath, pattern));
				}
			}
		}
		return paths;
	}

	public static class FeatureMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tit ="";
			String kw="";
			String[] fields=line.split("`");
			for (int i = 0; i < fields.length; i++) {
				if(fields[i].startsWith("tit=")){
					tit=fields[i];
					continue;
				}
				if(fields[i].startsWith("kw=")){
					kw=fields[i];
					continue;
				}
				if(!tit.equals("")&&!kw.equals(""))break;
			}
			String[] tit_kw = tit.substring(4).split(" ");
			String[] kw_kw = kw.substring(3).split(" ");
			for (int i = 0; i < tit_kw.length; i++) {
				
				if(tit_kw[i].startsWith("�"))continue;
				if(tit_kw[i].startsWith("�ָ"))continue;
				if(tit_kw[i].startsWith("오"))continue;
				if(tit_kw[i].startsWith("ﺑ"))continue;
				if(tit_kw[i].startsWith("내"))continue;
				if(tit_kw[i].startsWith("<feff>"))continue;
				tit_kw[i]=tit_kw[i].replace("'", "");
				if(tit_kw[i].trim().equals(""))continue;
				context.write(new Text(tit_kw[i] + "_1"), new Text());
			}
			for (int i = 0; i < kw_kw.length; i++) {
				
				if(kw_kw[i].startsWith("�"))continue;
				if(kw_kw[i].startsWith("�ָ"))continue;
				if(kw_kw[i].startsWith("오"))continue;
				if(kw_kw[i].startsWith("ﺑ"))continue;
				if(kw_kw[i].startsWith("내"))continue;
				if(kw_kw[i].startsWith("<feff>"))continue;				
				kw_kw[i]=kw_kw[i].replace("'", "");
				if(kw_kw[i].trim().equals(""))continue;
				context.write(new Text(kw_kw[i] + "_2"), new Text());
			}
		}
	}
	public static class FeatureCombiner extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, new Text());
		}
	}
	public static class FeatureReducer extends Reducer<Text, Text, Text, LongWritable> {
		int id = 1;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(key.toString().split("\t").length==1){
				context.write(key, new LongWritable(id));
				id+=1;
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		Path dataSource = new Path("/user/zengmx/0731/");
		Path outputPath = new Path("/user/zengmx/FeatureOutput_0731");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath))
			fs.delete(outputPath);
		List<Path> inputPaths = getFilesUnderFolder(fs, dataSource, "unformat");
		fs.close();
		Job job = Job.getInstance(conf, "FeatureTask");
		job.setJarByClass(FeatureTask.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(FeatureMapper.class);
		job.setCombinerClass(FeatureCombiner.class);
		job.setReducerClass(FeatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < inputPaths.size(); i++) {
			FileInputFormat.addInputPath(job, inputPaths.get(i));
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
