
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
    
    List<double[]> centroids = new ArrayList<>();
    private final IntWritable centroidIndex = new IntWritable();
    private final Text point2 = new Text();
    
    @Override
    public void setup(Context context) throws FileNotFoundException, IOException {
        
        Scanner in = new Scanner(new File("/Stuff/Courses/Projects/MapReduce/KMeansData/centroids/centroids.txt"));
        while(in.hasNextLine()) {
            
            String[] line = in.nextLine().split(",");
            double[] cent = new double[line.length];
            for (int i = 0; i < line.length; i++) {
                cent[i] = Double.parseDouble(line[i]);
            }
            this.centroids.add(cent);            
        }
    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] line = value.toString().split(",");
        double[] point = new double[line.length];
        int count = 0;
        double tempDist = Double.MAX_VALUE;
        int belongsToCentroid = 0;
                
        for (int i = 0; i < line.length; i++) {
            point[i] = Double.parseDouble(line[i]);
        }
        
        for (double[] centroid : centroids) {
            
            count++;
            double eucDist = euclidianDistance(point, centroid);
            if(eucDist < tempDist) {
                tempDist = eucDist;
                belongsToCentroid = count;
            }
        }
        centroidIndex.set(belongsToCentroid);
        point2.set(value.toString());
        //context.write(new IntWritable(belongsToCentroid), new Text(value.toString()));
               
        Configuration conf = context.getConfiguration();
        conf.set("line.length", Integer.toString(line.length));
        context.write(centroidIndex, point2);
        
    }
    
    private double euclidianDistance(double[] set1, double[] set2) {
        
        double sum = 0;
        for (int i = 0; i < set1.length; i++) {
            double diff = set1[i] - set2[i];
            sum += diff * diff;
        }
        
        return Math.sqrt(sum);        
    }
    
    
    
    
}
