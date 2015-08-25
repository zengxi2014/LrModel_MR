package com.uc.bigdata.exercise;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SamplePartition extends HashPartitioner<Text,Text>{
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		int p=0;
		if(key.toString().startsWith("0001"))p=1;
		if(key.toString().startsWith("0002"))p=2;
		if(key.toString().startsWith("0003"))p=3;
		if(key.toString().startsWith("0004"))p=4;
		if(key.toString().startsWith("0005"))p=5;
		if(key.toString().startsWith("0006"))p=6;
		if(key.toString().startsWith("0007"))p=7;
		if(key.toString().startsWith("0008"))p=8;
		if(key.toString().startsWith("0009"))p=9;
		if(key.toString().startsWith("0010"))p=10;
		if(key.toString().startsWith("0011"))p=11;
		if(key.toString().startsWith("0012"))p=12;
		if(key.toString().startsWith("0013"))p=13;
		if(key.toString().startsWith("0014"))p=14;
		if(key.toString().startsWith("0015"))p=15;
		if(key.toString().startsWith("0016"))p=16;
		if(key.toString().startsWith("0017"))p=17;
		if(key.toString().startsWith("0018"))p=18;
		if(key.toString().startsWith("0019"))p=19;
		if(key.toString().startsWith("0020"))p=20;
		if(key.toString().startsWith("0021"))p=21;
		if(key.toString().startsWith("0022"))p=22;
		if(key.toString().startsWith("0023"))p=23;
		if(key.toString().startsWith("0024"))p=24;
		if(key.toString().startsWith("0025"))p=25;
		if(key.toString().startsWith("0026"))p=26;
		if(key.toString().startsWith("0027"))p=27;
		if(key.toString().startsWith("0028"))p=28;
		if(key.toString().startsWith("0029"))p=29;
		if(key.toString().startsWith("0030"))p=30;
		if(key.toString().startsWith("0031"))p=31;
		if(key.toString().startsWith("0032"))p=32;
	//	if(key.toString().startsWith("0033"))p=33;
	//	if(key.toString().startsWith("0034"))p=34;
	//	if(key.toString().startsWith("0035"))p=35;
		return p;
	}
}
