
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;


public class KMeansDriver {
    
    public static void main(String[] args) throws IOException, 
            InterruptedException, ClassNotFoundException {
        
        Configuration conf = new Configuration();
        int iteration = 1;
        //conf.set("iteration", iteration + "");        
        
        Path dataPath = new Path("/Stuff/Courses/Projects/MapReduce/KMeansData/data");
        Path centroidPath = new Path("/Stuff/Courses/Projects/MapReduce/KMeansData/centroids");
        conf.set("centroid.path", centroidPath.toString());
        Path outPath = new Path("/Stuff/Courses/Projects/MapReduce/KMeansData/op");
        
        Job job = new Job(conf);
        job.setJobName("KMeans");
        
        
        job.setJarByClass(KMeansDriver.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        //job.setJarByClass(KMeansMapper.class);
        
        FileInputFormat.addInputPath(job, dataPath);
        //FileSystem fs = FileSystem.get(conf);
        //if(fs.exists(outPath)) fs.delete(outPath, true);
        //if(fs.exists(centroidPath)) fs.delete(centroidPath, true);
        //if(fs.exists(dataPath)) fs.delete(dataPath, true);
        FileOutputFormat.setOutputPath(job, outPath);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.waitForCompletion(true);
        
        long counter = job.getCounters().findCounter(KMeansReducer.Counter.COUNTER).getValue();
        iteration++;
        
        while(counter > 0) {
            
            conf = new Configuration();
            conf.set("centroid.path", centroidPath.toString());
            conf.set("iteration", iteration + "");
            job = Job.getInstance(conf);
            job.setJobName("KMeans" + iteration);
            
            job.setJarByClass(KMeansDriver.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            
            
            
        }
        
        
        
    }
    
    
    
}
