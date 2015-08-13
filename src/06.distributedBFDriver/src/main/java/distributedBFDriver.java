import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class distributedBFDriver {
  
  public static int getOptimalBFSize(int numRecords, float falsePositiveRate) {
    return  (int) (-numRecords * (float) Math.log(falsePositiveRate) / Math.pow(Math.log(2), 2));
  }
  public static int getOptimalK(float numMembers, float vectorSize) {
    return (int) (Math.round(vectorSize / numMembers * Math.log(2)));
  }
  
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    if (otherArgs.length != 4){
      System.err.println("Usage: distributedBFDriver <in> <out> <numMembers> <falsePositiveRate>");
      System.exit(2);
    }
    
    Path inputFile = new Path(otherArgs[0]);
    Path outputFile = new Path(otherArgs[1]);
    int numMembers = Integer.parseInt(otherArgs[2]);
    float falsePositiveRate = Float.parseFloat(otherArgs[3]);
    
    
    // calculate optimal vector size & K values based on approximations
    int vectorSize = getOptimalBFSize(numMembers, falsePositiveRate);
    int nbHash = getOptimalK(numMembers, vectorSize);
    
    // create new bloom filter
    BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);    
    
    System.out.println("Training Bloom filter of size " + vectorSize
        + " with " + nbHash + " hash functions, " + numMembers
        + " approximate number of records, and " + falsePositiveRate
        + " false positive rate");
    
    String line = null;
    int numElements = 0;
    FileSystem fs = FileSystem.get(conf);
    
    for (FileStatus stat : fs.listStatus(inputFile)){
      System.out.println("Reading " + stat.getPath());
      
      // set buffered input stream reader
      BufferedReader rdr = new BufferedReader(new InputStreamReader (fs.open(stat.getPath())));   
      
      while ((line = rdr.readLine()) != null) {
        filter.add(new Key(line.getBytes()));
        ++numElements;
      }
      
      rdr.close();
    }
    
    //pass the stream to the filter's write menthod
    FSDataOutputStream outputStream = fs.create(outputFile);
    filter.write(outputStream);
    
    outputStream.flush();
    outputStream.close();
    
    System.exit(0);
  }
  
}
