
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import com.amazonaws.auth.AWSCredentialsProvider;
public class TestDownload {
    public static String getObjectFromBucket(String bucketName, String keyName) throws IOException {
        AmazonS3 S3;
        AWSCredentialsProvider credentialsProvider;
        credentialsProvider = new ProfileCredentialsProvider("C:\\Users\\idona\\.aws\\config.txt","default");
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        GetObjectRequest request = new GetObjectRequest(bucketName,keyName);
        S3Object s3Object = S3.getObject(request);
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
        String curr_dir = System.getProperty("user.dir");
        String destination_path = curr_dir + File.separator+keyName;
        FileOutputStream fileOutputStream = new FileOutputStream(destination_path);

        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = s3ObjectInputStream.read(buffer)) != -1) {
            fileOutputStream.write(buffer, 0, bytesRead);
        }
        fileOutputStream.close();
        return destination_path;
    }
    public static String getObjectFromBucket2(String key_name,String bucket_name){
        System.out.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        try {
            S3Object o = s3.getObject(bucket_name, key_name);
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(key_name);
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return key_name;
    }

    private static void fileToSet(String filePath, Set<String> stopWords) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split("\\s+"); // Split by whitespace characters
                // Add the word to the set
                stopWords.addAll(Arrays.asList(words));
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        Set<String> stopWords = new HashSet<>();
        try {
            String filePath = getObjectFromBucket2("eng-stopwords.txt","dsp-2gram2");
            fileToSet(filePath,stopWords);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
