/**
 * 
 */
package com.rax.aws.awsjavasampleapp.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * @author MuOthman
 *
 */
public class S3AppExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Region region = Region.US_EAST_1;

		S3Client s3 = S3Client.builder().region(region).build();

		String bucket = "bucket" + System.currentTimeMillis();
		String key = "key-sample-object-file";

		tutorialSetup(s3, bucket, region);

		System.out.println("Uploading object...");

		s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
				RequestBody.fromString("Testing with the AWS SDK for Java"));

		System.out.println("Upload complete");
		System.out.printf("%n");

		// List Existing Buckets after adding new one
		System.out.println("List All Buckets");
		listBuckets(s3);
		
		System.out.println("Delete Created Bucket");
		cleanUp(s3, bucket, key);

		System.out.println("Closing the connection to Amazon S3");
		s3.close();
		System.out.println("Connection closed");
		System.out.println("Exiting...");
	}

	public static void tutorialSetup(S3Client s3Client, String bucketName, Region region) {
		try {
			s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName)
					.createBucketConfiguration(CreateBucketConfiguration.builder().build()).build());
			System.out.println("Creating bucket: " + bucketName);
			s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build());
			System.out.println(bucketName + " is ready.");
			System.out.printf("%n");
		} catch (S3Exception e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}

	public static void cleanUp(S3Client s3Client, String bucketName, String keyName) {
		System.out.println("Cleaning up...");
		try {
			System.out.println("Deleting object: " + keyName);
			DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(keyName)
					.build();
			s3Client.deleteObject(deleteObjectRequest);
			System.out.println(keyName + " has been deleted.");
			System.out.println("Deleting bucket: " + bucketName);
			DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
			s3Client.deleteBucket(deleteBucketRequest);
			System.out.println(bucketName + " has been deleted.");
			System.out.printf("%n");
		} catch (S3Exception e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
		System.out.println("Cleanup complete");
		System.out.printf("%n");
	}

	public static void listBuckets(S3Client s3Client) {
		ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
		ListBucketsResponse listBucketsResponse = s3Client.listBuckets(listBucketsRequest);
		listBucketsResponse.buckets().stream().forEach(x -> System.out.println(x.name()));
	}
}
