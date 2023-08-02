package com.app.raghu;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Example {

    public static void main(String[] args) throws Exception {
        // Replace 'YOUR_REGION' with the AWS region you want to work with (e.g., "us-east-1")
        Region region = Region.of("ap-south-1");

        // Create an S3 client using default credentials provider
        S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        String bucketName = "java-buck-raghu";
        String key = "sample.txt"; // The object key (filename) in the bucket
        String localFilePath = "D:\\s3data\\sample.txt"; // Replace with the path of the file you want to upload

        // Upload an object to S3
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(), RequestBody.fromInputStream(new FileInputStream(localFilePath), new File(localFilePath).length()));

        System.out.println("File uploaded successfully.");

        // Download an object from S3
        String downloadFilePath = "D:\\s3data\\downloads\\downloaded-file.txt"; // Replace with the desired local path to save the downloaded file
        ResponseBytes<GetObjectResponse> response = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(), ResponseTransformer.toBytes());

        // Write the response body to a local file
        byte[] objectBytes = response.asByteArray();
        FileOutputStream fos = new FileOutputStream(downloadFilePath);
        fos.write(objectBytes);
        fos.close();

        System.out.println("File downloaded successfully.");

        // List objects in a bucket
        ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build());

        System.out.println("Objects in bucket '" + bucketName + "':");
        for (S3Object object : listObjectsResponse.contents()) {
            System.out.println(" - " + object.key());
        }

        // Don't forget to close the S3 client when you're done.
        s3Client.close();
    }
}

