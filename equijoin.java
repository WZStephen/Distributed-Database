//Name: Weichi Zhao
//Class: CSE512
//Time: M W 9:00-10:15
//Student ID: 1209692845

package equijoin;
import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class equijoin {
	public static void main(String[] args) throws Exception {
		  
	    Configuration conf = new Configuration();
	    
	    Job job = Job.getInstance(conf,"equijoin");
	    
	    job.setJarByClass(equijoin.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //args[1] --> input (localhost:9000/Sample.txt) 
	    //args[2] --> output(localhost:9000/output)
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
	  public static class Map extends Mapper<Object, Text, Object, Text>{
		  
		    private Text text = new Text();
		    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
		    StringTokenizer token = new StringTokenizer(value.toString(),"\n");
		    
		    //Read and Split
		    while (token.hasMoreTokens()) {
		    	text.set(token.nextToken());
			    String[] ele = text.toString().split(", ");
			    context.write(new Text(ele[1]), text);
		    }	    
	      }    
	  }

	  public static class Reduce extends Reducer<Text,Text,Text,Text> {
		  
	       int location = -1;
	       private Text final_str = new Text();
	
	       public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	    	   
	       List<String> t1_row = new ArrayList<String>();
	       List<String> t2_row = new ArrayList<String>();
	       
	       String output = "";
	       String table1 = "";
	       String table2 = "";
	       
	       //for each elements in values, concatenate them into one
	       for (Text ele : values) {	 
		        location++;
		        String[] tokens = ele.toString().split(", ");
		        if (table1.equals("")){
		        	table1 = tokens[0];
		        }else if (table2.equals("") && !(tokens[0].equals(table1))){
		        	table2 = tokens[0]; 
		        }
		        if (tokens[0].equals(table1)){
		        	t1_row.add(ele.toString());
		        }
		        else if (tokens[0].equals(table2)){
		        	t2_row.add(ele.toString());
		        }
	       }
	      
	       //for each elements in table1 and tabl2, concatenate them into one
	      for (String ele1 : t1_row){
	        for (String ele2: t2_row){
	          
	        	output = output + ele2 + ", " + ele1 +"\n";
	          
	         }
	      }
	      	
	      if(location<=0){
	          return;
	      }else{
	    	  output = output.trim();
	    	  final_str.set(new Text(output));
	          context.write(null,final_str);  
	      }
	    }
	  }  

}