package org.myorg;
// importing the library for needed the  driver class
//importing the configuration class from Apache Hadoop library
import org.apache.hadoop.conf.Configuration;
// importing the path class,this is a key class of Hadoop
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//represents a Mapreduce job, which is unit of work
import org.apache.hadoop.mapreduce.Job;
/*FileInputFormat class is built in input format, for Hadoop MapReduce jobs 
that reads data from one or more input files and splits them into smaller chunks called input splits */
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//FileOutputFormat class is used as a base class for output formats that write data to files
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// accessing and manipulates files and directoreis
import org.apache.hadoop.fs.FileSystem;

//main class in map reduce program
public class SalesProfit_Driver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		// class is used to store configuration settings for a Hadoop 
		Configuration confg = new Configuration();
		
		// if arguments are less than 3 , so it raise error for wrong arguments
		if(args.length!=3)
		{
			System.err.println("Usage: Product subcategory Sales_profit <Input Path>  <Output Path>");
			System.exit(-1);
			
		}
		// job class represents a MapReduce job, which consists of a map phase and a reduce phase
		Job job1;
		// confg, is used to provide the job's configuration settings
		job1=Job.getInstance(confg ,"Product subcategory Saleprofit Summary");
		//a JAR (Java Archive) file is a package file that contains compiled Java classes
		job1.setJarByClass(SalesProfit_Driver.class);
		
		//FileInputFormat class is used to define the input format for a Hadoop job
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		
		// FileOutputFormat class is used to define the output format job and passed argument 
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		
		
		//a mapper is a type of function that takes a set of input data and transforms
		job1.setMapperClass(SalesProfit_Mapper.class);
		
		/*The Combiner class is an abstract class in Hadoop that 
		provides a framework for implementing a combiner function */
		job1.setCombinerClass(SalesProfit_Reducer.class);
		/* Set the custom sorting comparator for the job, which will be used to sort the 
		intermediate key-value pairs before they are sent to the reducer */
		job1.setSortComparatorClass(SalesSortComparator.class);
		/* Set the reducer class for the job, which will be responsible for processing 
		and aggregating the sorted intermediate key-value pairs*/
		job1.setReducerClass(SalesProfit_Reducer.class);
		//the output key class specifies the type of the keys that will be output by the mapper or reducer
		job1.setOutputKeyClass(Text.class);
		//sets the output value class for a Hadoop job to the Text class
		job1.setOutputValueClass(Text.class);
		
		// if file exist in Hadoop virtual machine then delete first
		FileSystem hdfs = FileSystem.get(confg);
		Path OutputDir= new Path(args[2]);
		
		if (hdfs.exists(OutputDir))
		{
			hdfs.delete(OutputDir,true);
			
		}
		// exits the current virtual machine process
		System.exit(job1.waitForCompletion(true)?0:1);

	}

}
