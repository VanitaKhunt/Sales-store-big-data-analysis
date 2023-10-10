// package name is used for user defined classes, it's common to use a unique package name for our classes
package org.myorg;

//importing the IOException class from the java.io package,  IOException class is a checked exception.
import java.io.IOException;
//It is use for Long integer as a writable object
import org.apache.hadoop.io.LongWritable;
//It is use for String writable object
import org.apache.hadoop.io.Text;
//It defines interface for mapper, which is responsible processing input data and
//emitting intermediate key-value pairs for further processing
import org.apache.hadoop.mapreduce.Mapper;
//A class to represent a date (year, month, and day) without time information
import java.time.LocalDate; 
//A class for parsing and formatting dates according to specified patterns
import java.time.format.DateTimeFormatter; 


/*class name is Product_Mapper that extends the Mapper class Hadoop
 * <> in between first argument LongWritable is document Id
 * Second argument contains of document
 * Third argument is for Output key from Mapper class
 *Forth argument is for Output Value from Mapper class
 * Mapper output context key and value variables type should be match with reducer input variables
 * here both key and value types are Text */
public class SalesProfit_Mapper extends Mapper<LongWritable,Text,Text,Text> 
{
	//private declaration for use only for this class 
	//subcat_date declare for key variable that would be hold combination of product's subcategory name + order date(Month-Year)
	private Text subcat_date = new Text();
	//This variable contain Sales and profit amount locally
	private Text sale_profitamt = new Text();
	//@ override is java annotation, intend to override the method same name and signature 
	@Override
	// map method in Mapper class key is Longwritable type, Value is Text and context,provides access to output collected
	public void map(LongWritable key ,Text Value,Context context)
	throws IOException, InterruptedException
	/*IOException for common checked exception it may raise by map class
	InterruptedException, it would be generated if thread is interrupted while waiting or sleeping */
	{
		//split records by comma separate and value would be stored in arraylist name by line
		String[] line = Value.toString().split(",");
		// array index start from 0 so in column 16 means index 15 is subcategory name 
		//and column 3 means index 2 for order date
		String subcategoryName = line[15].trim();
		String date = line[2].trim();
	 
		// in our data date format is not unique, so we need to convert in unique format
	   DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("M/d/yyyy");
	   // we set our date format in year-month
	   DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM");
	   LocalDate localDate = LocalDate.parse(date, inputFormatter);
	   String monthYear = localDate.format(outputFormatter);
	    
	   //merge in subnm_date, concating with \t means by tab
	    String subnm_date = subcategoryName + '\t' + monthYear;
	    
		
		//sale_profit will be store product subcategory's Sales and profit amount
		String sale_profit =line[17].trim() + '\t' + line[20].trim();
		
		// .set method is used to set the value of subnm_date to subcat_date,that will be use for output key for the mapper
		subcat_date.set(subnm_date);
		sale_profitamt.set(sale_profit);
		// writes the key value pair to the output of the Mapper
		context.write(subcat_date, sale_profitamt);
	}
}
