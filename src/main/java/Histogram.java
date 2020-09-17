import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*; 


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/* single color intensity */
class Color implements WritableComparable<Color> {
	public short type;       /* red=1, green=2, blue=3 */
	public short intensity;  /* between 0 and 255 */
	/* need class constructors, toString, write, readFields, and compareTo methods */
	public Color() {
		this.type = 0;
		this.intensity = 0;
	}
	public Color(short type,short intensity){
		this.type = type;
		this.intensity = intensity;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(type);
		out.writeShort(intensity);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readShort();
		intensity = in.readShort();
	}
	@Override
	public int compareTo(Color arg0) {
		if(type < arg0.type){
			return -1;
		}else if ((type > arg0.type)){
			return 1;
		}else{
			if(intensity < arg0.intensity) {
				return -1;
			}else if(intensity > arg0.intensity) {
				return 1;
			}else {
				return 0;
			}
		}
	}
	@Override
	public String toString() {
		return type+"	"+intensity+"	";
	}
	
	@Override
	public int hashCode() {
		int primeNo = 10009;
		int hashCodeNo = 0;
		hashCodeNo = (primeNo * type) + intensity;
		return hashCodeNo;
	}
}

public class Histogram {
	public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
		private static final IntWritable one =  new IntWritable(1);

		@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException {
			String[] data = value.toString().split(",");
			short densityRed = new Short(data[0]);
			short densityGreen = new Short(data[1]);
			short densityBlue = new Short(data[2]);

			context.write(new Color(new Short("1"), densityRed),one);
			context.write(new Color(new Short("2"), densityGreen), one);
			context.write(new Color(new Short("3"), densityBlue), one);
		}
	}

	public static class HistogramCombiner extends Reducer<Color,IntWritable,Color,IntWritable>{
		@Override
		public void reduce ( Color key, Iterable<IntWritable> values, Context context )
				throws IOException, InterruptedException {
			int count =0;
			for(IntWritable value :values) {
				count += value.get();        
			}
			context.write(key,new IntWritable(count));
		}
	}

	public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {

		@Override
		public void reduce ( Color key, Iterable<IntWritable> values, Context context )
				throws IOException, InterruptedException {
			long count =0;
			for(IntWritable value :values) {
				count += value.get();        
			}
			context.write(key,new LongWritable(count));
		}
	}

	public static class HistogramMapper1 extends Mapper<Object,Text,Color,IntWritable> {
		private static Hashtable<Color,Integer> hcolor;
		private static int count=0;
		private static final int reduceCount = 768;
		@Override
		protected void setup (Context context)
				throws IOException, InterruptedException {
			hcolor = new Hashtable<Color,Integer>();
		}

		@Override
		protected void cleanup (Context context)
				throws IOException, InterruptedException {
			for(Color c : hcolor.keySet() )
			{
				context.write(c,new IntWritable(hcolor.get(c)));
			}
			hcolor.clear();
		}

		@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException {
			if(count >= reduceCount) {
				cleanup(context);
				count=0;
			}
			count++;
			String[] data = value.toString().split(",");
			short densityRed = new Short(data[0]);
			short densityGreen = new Short(data[1]);
			short densityBlue = new Short(data[2]);

			List<Color> colors = new ArrayList<>();
			colors.add(new Color(new Short("1"),densityRed));
			colors.add(new Color(new Short("2"),densityGreen));
			colors.add(new Color(new Short("3"),densityBlue));

			colors.forEach(color -> {
				if(hcolor.contains(color)){
					hcolor.put(color, hcolor.get(color) + 1);
				}else {
					hcolor.put(color,1);
				}
			});
		}
	}
	public static class HistogramReducer1 extends Reducer<Color,IntWritable,Color,LongWritable> {

		@Override
		public void reduce ( Color key, Iterable<IntWritable> values, Context context )
				throws IOException, InterruptedException {
			long count =0;
			for(IntWritable value :values) {
				count += value.get();        
			}
			context.write(key,new LongWritable(count));
		}
	}

	public static void main ( String[] args ) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("Histogram");
		job.setJarByClass(Histogram.class);

		job.setOutputKeyClass(Color.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapOutputKeyClass(Color.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(HistogramMapper.class);
		job.setCombinerClass(HistogramCombiner.class);
		job.setReducerClass(HistogramReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);

		Job job1 = Job.getInstance();
		job1.setJobName("Histogram");
		job1.setJarByClass(Histogram.class);

		job1.setOutputKeyClass(Color.class);
		job1.setOutputValueClass(LongWritable.class);

		job1.setMapOutputKeyClass(Color.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setMapperClass(HistogramMapper1.class);
		job1.setReducerClass(HistogramReducer1.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1,new Path(args[0]));
		FileOutputFormat.setOutputPath(job1,new Path(args[1]+"2"));
		job1.waitForCompletion(true);
	}
}






























