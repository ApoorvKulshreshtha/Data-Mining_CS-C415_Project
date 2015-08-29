import java.io.File;
import java.io.IOException;


import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class BayesianClassifierMapReduce extends Configured implements Tool{

	private static final String inputdatapath="/home/hduser/Apoorv/BayesianMapReduce/InputData/crime_incident.csv";
	private static final String outputpath="/home/hduser/Apoorv/BayesianMapReduce/output";
	public static class Map extends Mapper<LongWritable,Text,Text,Text> 
	{
		File newf=new File("/home/hduser");
		
		@SuppressWarnings("unchecked")
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{
			
			
			String[] split=value.toString().split(",");
			int len=split.length-1;
			Integer total_crime=new Integer(split[len]);
			int total=total_crime.intValue();
			int i;
			for(i=1;i<len;i++)
			{
				Integer temp=new Integer(split[i]);
				int val=temp.intValue();
				double prob=(float)val/total;
				Text city_no=new Text((new Integer(i)).toString());
				Text crime_prob=new Text(split[0]+","+(new Double(prob).toString())+","+total_crime.toString()+","+temp.toString());
				context.write(city_no, crime_prob);
			}
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			double prob_city;
			double prob_crime;
			double newprob;
			double maxprob = 0.0;
			String maxprob_crime = " ";
			int flag=0,total=0,crime_tot=0,i;
			//Iterator iter1= values.iterator<Text>;
			String[][] split2=new String[50][50];
			int c=0;
			for(Text iter1 :values)
			{
				split2[c]=iter1.toString().split(",");
				int l=split2[c].length-1;
				Integer t=new Integer(split2[c][l-1]);
				total=total+t.intValue();
				Integer cr_val=new Integer(split2[c][l]);
				crime_tot=crime_tot+cr_val.intValue();
				c++;
			}
			prob_city=(float)crime_tot/total;
			
			
			for(i=0;i<c;i++)
			{
				//String[] split2=value.toString().split(",");
				prob_crime=(new Double(split2[i][2])).doubleValue()/total;
				newprob=(new Double(split2[i][1]).doubleValue())*prob_crime/prob_city;
				if(flag == 0)
				{
					maxprob=newprob;
					maxprob_crime=(split2[i][0]).toString();
					flag=1;
				}
				else
				{
					if(newprob>maxprob)
					{
						maxprob=newprob;
						maxprob_crime=(split2[i][0]).toString();
					}
				}
			}
			Text val3=new Text(key.toString() + " " + maxprob_crime+" "+(new Double(maxprob)).toString());
			context.write(null, val3);
		}
	}
	public int run(String[] args)throws Exception 
	{
		Configuration conf=new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(BayesianClassifierMapReduce.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(inputdatapath));
		FileOutputFormat.setOutputPath(job,new Path(outputpath));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	public static void main(String[] args)throws Exception
	{
		int result=ToolRunner.run(new Configuration(),new BayesianClassifierMapReduce(),args);
		System.exit(result);
	}
}
