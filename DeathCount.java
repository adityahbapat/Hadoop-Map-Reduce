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

public class DeathCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Text country = new Text(value.toString().split(",")[3]);
      String wd = value.toString().split(",")[6];
      if (wd == "Confirmed" || wd == "" || wd == "NA") {
        return;
      }
      System.out.println("->wd:"+ wd);
      try {
        IntWritable confirmed = new IntWritable((int) Float.parseFloat(value.toString().split(",")[6]));
        if (country != new Text("Country/Region")) {
          context.write(country, confirmed);
        }
      } catch (Exception e) {
        System.out.println(wd + " :Cannot be formatted " + e);
      }

    }
  }

  public static class IntDeathReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      int deathSum = 0;
      for (IntWritable val : values) {
        deathSum += val.get();
      }
      System.out.println(key +":" + deathSum);
      result.set(deathSum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "death count");
    job.setJarByClass(DeathCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntDeathReducer.class);
    job.setReducerClass(IntDeathReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJar("DeathCount.jar");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}