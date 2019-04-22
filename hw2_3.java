import java.io.IOException;
import java.util.StringTokenizer;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import com.opencsv.CSVParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class Movie {
	public static class Pair implements Writable
	{
		String s;
		FloatWritable f;
		Pair(){}
		Pair(String s_, FloatWritable f_)
		{
			s = new String(s_);
			f = new FloatWritable(f_.get());
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.s);
			out.writeUTF(this.f.toString());
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			this.s = in.readUTF();
			this.f.set(Float.parseFloat(in.readUTF()));
		}
	}
	public static class FilmMapper
    extends Mapper<Object, Text, LongWritable, Pair >{
		
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			String[] units = new CSVParser().parseLine(value.toString());
			
			if(units[9].equals("production_companies")) return;
			
			JsonArray companies = new JsonParser().parse(units[9]).getAsJsonArray();
			float score = Float.parseFloat(units[18]);
			if(score >= 6.5)
			for(int i = 0;i < companies.size(); ++i)
			{
				JsonObject obj = companies.get(i).getAsJsonObject();
				context.write(new LongWritable(obj.get("id").getAsLong()), 
						new Pair(obj.get("name").getAsString(), 
								new FloatWritable(Float.parseFloat(units[12]))));
			}
		}
  }
	public static class FloatSumReducer
    extends Reducer<LongWritable, Pair, LongWritable, Pair> {
	private FloatWritable result = new FloatWritable();

	public void reduce(LongWritable key, Iterable<Pair> values,
                    Context context
                    ) throws IOException, InterruptedException {
   int sum = 0;
   for (Pair val : values) {
     sum += val.f.get();
   }
   result.set(sum);
   context.write(key, new Pair(values.iterator().next().s, result));
 }
}
	
	  public static void main(String[] args) throws Exception {
		    if(args.length!=2){
		        System.err.println("Uage: wordcount <in> <out>");
		        System.exit(2);
		    }
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Movie");
		    job.setJarByClass(Movie.class);
		    job.setMapperClass(FilmMapper.class);
		    job.setCombinerClass(FloatSumReducer.class);
		    job.setReducerClass(FloatSumReducer.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(Pair.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
