import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BigDataTest {

	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		boolean firstLine = true;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			if (!firstLine) {
				String line = value.toString();
				String[] split = line.split("\t");

				String solver = new String(split[0]);
				String time = new String(split[11]);
				String result = new String(split[14]);

				Text s = new Text(solver.toString());
				Text t = new Text(time.toString());

				if (result.equals("solved"))
					// context.write(new MyWritable(s, t), new Text(time));
					context.write(s, t);
			}

			if (firstLine)
				firstLine = !firstLine;

		}
	}

	static class MyReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2) throws IOException, InterruptedException {

			String chainOfTimes = new String();

			for (Text time : arg1) {
				chainOfTimes += time.toString() + " ";
			}

			arg2.write(arg0, new Text(chainOfTimes));

		}
	}

	static class MapperTable extends Mapper<Text, Text, LongWritable, Text> {

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {

			String[] splittedTimes = value.toString().split(" ");
			String solver = new String(key.toString());

			// String[] splittedTimes = splittedRow[1].toString().split("#");

			context.write(new LongWritable(0), new Text(solver));

			for (int i = 1; i < splittedTimes.length; i++) {
				String formattedRow = new String(solver + "&" + splittedTimes[i]);
				context.write(new LongWritable(i), new Text(formattedRow.toString()));
			}
		}
	}

	static class ReducerTable extends Reducer<LongWritable, Text, Text, Text> {

		boolean solvers = true;

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			if (solvers) {
				String solverLine = new String();
				for (Text value : values) {
					solverLine += value.toString() + "\t";
				}

				context.write(new Text(key + ""), new Text(solverLine.toString()));
				solvers = false;

			}

			String timeLine = new String();
			for (Text value : values) {

				String[] split = value.toString().split("&");
				timeLine += split[1] + "\t";
			}

			context.write(new Text(key + ""), new Text(timeLine.toString()));

			// int count = 0;
			//
			// for (Text text : arg1) {
			// String s = new String(text + "\t" + count);
			// count++;
			// arg2.write(arg0, new Text(s.toString()));
			// }
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path inputTemp = new Path(files[1]);
		Path outputFinal = new Path(files[2]);

		Job job = Job.getInstance(conf, "Test");
		job.setJarByClass(BigDataTest.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, inputTemp);

		boolean success = job.waitForCompletion(true);

		if (success) {
			Job jobTable = Job.getInstance(conf, "Table");
			jobTable.setMapperClass(MapperTable.class);
			jobTable.setReducerClass(ReducerTable.class);

			jobTable.setInputFormatClass(KeyValueTextInputFormat.class);

			jobTable.setMapOutputKeyClass(LongWritable.class);
			jobTable.setMapOutputValueClass(Text.class);

			jobTable.setOutputKeyClass(Text.class);
			jobTable.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(jobTable, inputTemp);
			FileOutputFormat.setOutputPath(jobTable, outputFinal);
			success = jobTable.waitForCompletion(true);
		}

		if (success)
			System.exit(1);
		else
			System.exit(0);
	}

}