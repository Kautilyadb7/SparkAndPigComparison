package spark.assignment2;

//Importing all the required Spark packages

import org.apache.spark.SparkConf; 
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.function.Function;

public class AssignmentProblem2 {

	public static void main(String[] args) {

		//Instantiating the spark conf instance

		SparkConf conf = new SparkConf().setAppName("MySecondSparkProgram").setMaster("local[*]");
		System.out.println("Conf has been set");

		//Declaring a JavaRDD and initializing it to null

		JavaRDD<String> stringRDD = null;

		//Need a try catch block here so that any problem in creating Spark Context
		//	Or in reading the input file be handled gracefully 
		try {
			JavaSparkContext sc = new JavaSparkContext(conf); 
			System.out.println("Context has been initialized");

			//Reading the input data set as a text file in JavaRDD created

			stringRDD = sc.textFile("/user/root/spark_assignment/input_dataset/yellow_tripdata*");
			System.out.println("File has been read successfully"); 
		}catch(Exception e) {

			//returning the stack trace of Exception encountered 
			e.printStackTrace();

		}

		//Implementing filter transformation on JavaRDD which has input data set

		JavaRDD<String> filterRDD = stringRDD.filter(new Function<String,Boolean>(){

			@Override
			public Boolean call(String row) throws Exception {

				//Reading each record and splitting it by: "," //Adding the split record to an array 

				String[] rowValues = row.split(",");

				//Checking if the record under processing is not null
				//and has length at least = 17

				if(rowValues!=null && rowValues.length>=17) {

					//Below is the filter condition as per Problem no 2
					if("4".equals(rowValues[5])) {
						return true;
					}
				}
				return false;
			}
		});//lambda function ends here System.out.println("Writing the output now");

		//Writing the output to a text file which will be saved at location as given in command line - args[1]

		filterRDD.saveAsTextFile(args[1]);
		System.out.println("File has been written with output");

	}

}

