
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;


public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    List<double[]> oldCentroids = new ArrayList<>();
    Configuration conf = new Configuration();
    int arrLength = Integer.parseInt(conf.get("line.length"));
    int[] sumPoints = new int[arrLength];
    double[] nC = new double[arrLength];    
    
    public static enum Counter {
        COUNTER;
    }

    @Override
    public void setup(Context context) throws FileNotFoundException, IOException {

        System.out.println("sdshdjshjd");
        Scanner in = new Scanner(new File("/Stuff/Courses/Projects/MapReduce/KMeansData/centroids/centroids.txt"));
        while(in.hasNext()) {
            
            String[] line = in.nextLine().split(",");
            double[] cent = new double[line.length];
            for (int i = 0; i < line.length; i++) {
                cent[i] = Double.parseDouble(line[i]);
            }
            oldCentroids.add(cent);            
        }
    }
    
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
      
        int count = 0;
        
        for (Text val : values) {
            count++;
            String[] line = val.toString().split(",");
            double[] point = new double[line.length];
                        
            for (int i = 0; i < line.length; i++) {
                point[i] = Double.parseDouble(line[i]);
            }
            for (int i = 0; i < line.length; i++) {
                sumPoints[i] += point[i];
            }
            context.write(key, val);
        }
        for (int i = 0; i < arrLength; i++) {
            nC[i] = sumPoints[i] / count;
        }
        
        if(!vectorDifference(nC, oldCentroids.get(key.get())))
            context.getCounter(Counter.COUNTER).increment(1);
        
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
        
        Configuration conf = context.getConfiguration();
        Path centroids = new Path(conf.get("centroid.path"));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(centroids, true);
        
        FileWriter fileWriter = new FileWriter(centroids.toString());
        for (int i = 0; i < nC.length; i++) {
            fileWriter.append(Double.toString(nC[i]));
            fileWriter.append(",");
        }
        fileWriter.append("\n");
        
    }
    
    private boolean vectorDifference(double[] set1, double[] set2) {
        
        int flag = 0;
        
        for (int i = 0; i < set1.length; i++) {
            if((set1[i] - set2[i]) == 0)
                flag++;
        }
        if (flag == 0) return true;
        else return false;
    }
    
    
}
