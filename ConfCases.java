import java.io.IOException;
// import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConfCases {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String ObservationDate = value.toString().split(",")[1];
      if (ObservationDate == "ObservationDate" || ObservationDate == "" || ObservationDate == "NA") {
        return;
      }
      
      Text country = new Text(value.toString().split(",")[3]);
      String wd = value.toString().split(",")[5];
      if (wd == "Confirmed" || wd == "" || wd == "NA") {
        return;
      }
      System.out.println("->od:"+ ObservationDate);
      try {
        IntWritable confirmed = new IntWritable((int) Float.parseFloat(value.toString().split(",")[6]));
        if (country != new Text("Country/Region") && ObservationDate != "ObservationDate") {
          Text year = new Text(ObservationDate.split("/")[2]);
          Text countryYear = new Text(year.toString() + "-" + country.toString());
          context.write(countryYear, confirmed);
        }
      } catch (Exception e) {
        System.out.println(wd + " :Cannot be formatted " + e);
      }

    }
  }

  public static class IntConfReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      int ConfSum = 0;
      for (IntWritable val : values) {
        ConfSum += val.get();
      }
      System.out.println(key +":" + ConfSum);
      result.set(ConfSum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "conf cases");
    job.setJarByClass(ConfCases.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntConfReducer.class);
    job.setReducerClass(IntConfReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJar("ConfCases.jar");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}