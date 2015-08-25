package com.uc.bigdata.exercise;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.uc.bigdata.exercise.domain.NotPronDomains;
import com.uc.bigdata.exercise.domain.NotPronDomains2;
import com.uc.bigdata.exercise.domain.PronDomains;
import com.uc.bigdata.exercise.domain.PronDomains2;
import com.uc.bigdata.exercise.domain.PronDomains3;

public class SamplingTask {
	public static class SamplingMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			boolean flag = false;
			String line = value.toString();
			String url = "";
			String[] fields = line.split("`");
			String tit = "";
			String kw ="";
			for (int i = 0; i < fields.length; i++) {
				if (fields[i].startsWith("url=")) {
					url = fields[i];
					continue;
				}
				if (fields[i].startsWith("tit=")) {
					tit = fields[i];
					continue;
				}
				if(fields[i].startsWith("kw=")){
					kw=fields[i];
					continue;
				}
				if (!tit.equals("") && !url.equals("")&&!kw.equals(""))
					break;
			}
			List<String> matchDomains = new ArrayList<String>();
			for (int i = 0; i < NotPronDomains2.othersDomains.length; i++) {
				if (url.indexOf(NotPronDomains2.othersDomains[i].substring(0,
						NotPronDomains2.othersDomains[i].indexOf("``"))) != -1) {
					flag = true;
					matchDomains.add(NotPronDomains2.othersDomains[i]);
				}
			}
			/*for (int i = 0; i < PronDomains.pornDomains.length; i++) {
				if (url.indexOf(
						PronDomains.pornDomains[i].substring(0, PronDomains.pornDomains[i].indexOf("``"))) != -1) {
					flag = true;
					matchDomains.add(PronDomains.pornDomains[i]);
				}
			}
			for (int i = 0; i < PronDomains2.pornDomains.length; i++) {
				if (url.indexOf(
						PronDomains2.pornDomains[i].substring(0, PronDomains2.pornDomains[i].indexOf("``"))) != -1) {
					flag = true;
					matchDomains.add(PronDomains2.pornDomains[i]);
				}
			}
			for (int i = 0; i < PronDomains3.pornDomains.length; i++) {
				if (url.indexOf(
						PronDomains3.pornDomains[i].substring(0, PronDomains3.pornDomains[i].indexOf("``"))) != -1) {
					flag = true;
					matchDomains.add(PronDomains3.pornDomains[i]);
				}
			}*/
			if (flag) {
				if (matchDomains.size() == 1) {
					context.write(
							new Text(
									matchDomains.get(0)
											.substring(
													matchDomains.get(0).indexOf("``")
															+ 2)
											+ tit),
							new Text("category=" + matchDomains.get(0).substring(matchDomains.get(0).indexOf("``") + 2)
									+ "`" + line));
				} else {
					int len = matchDomains.size();
					String maxLenURL = matchDomains.get(0);
					int maxFields = matchDomains.get(0).split("\\.").length;
					for (int i = 1; i < len; i++) {
						if (matchDomains.get(i).split("\\.").length > maxFields) {
							maxFields = matchDomains.get(i).split("\\.").length;
							maxLenURL = matchDomains.get(i);
						} else if (matchDomains.get(i).split("\\.").length == maxFields) {
							if (matchDomains.get(i).length() > maxLenURL.length()) {
								maxFields = matchDomains.get(i).split("\\.").length;
								maxLenURL = matchDomains.get(i);
							}
						}
					}
					context.write(new Text(maxLenURL.substring(maxLenURL.indexOf("``") + 2) + tit+kw),
							new Text("category=" + maxLenURL.substring(maxLenURL.indexOf("``") + 2) + "`" + line));
				}
			}

		}
	}

	public static class SamplingReducer extends Reducer<Text, Text, Text, Text> {
		static int num = 0;
		int sampleSize = 1000000;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> valIter = values.iterator();
			if (valIter.hasNext() && num < sampleSize) {
				context.write(valIter.next(), new Text());
				num++;
			}

		}

	}

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

	public static void main(String[] args) throws Exception {
		Path dataSource = new Path("/user/uaewa/updc/sys_data/browse_data_hour/2015/07");
		Path outputPath = new Path("/user/zengmx/ClassifyOutput_07_01_31");
		Configuration conf = new Configuration();
//		conf.setInt("maxSamplePerCate", 1000000);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath))
			fs.delete(outputPath);
		List<Path> inputPaths = new ArrayList<Path>();
		inputPaths.addAll(getFilesUnderFolder(fs, dataSource, "part-"));
		fs.close();
		Job job = Job.getInstance(conf, "SamplingTask");
		job.setJarByClass(SamplingTask.class);
		job.setNumReduceTasks(33);
		job.setMapperClass(SamplingMapper.class);
		job.setReducerClass(SamplingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(SamplePartition.class);
		for (int i = 0; i < inputPaths.size(); i++) {
			FileInputFormat.addInputPath(job, inputPaths.get(i));
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
