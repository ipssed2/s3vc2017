// s3vc2017.cpp:  Application to test the AWS C++ library, for S3.
// Mark Riordan  2017-07-19

#include "stdafx.h"
#include <iostream>
#include <fstream>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Bucket.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/Object.h>

using namespace std;

class TestArgs {
public:
	string accessKeyId;
	string secretKey;
	string action;
	string bucket_name;
	string prefix;
	string localfile_name;
};

class S3Test {
	Aws::SDKOptions options;
	Aws::S3::S3Client *s3_client;
public:
	~S3Test() {
		delete s3_client;
	}
protected:
	void startup() {
		Aws::InitAPI(options);
	}

	void shutdown() {
		Aws::ShutdownAPI(options);
	}

	void authenticate(string accessKeyId, string secretKey) {
		Aws::Auth::AWSCredentials credentials(accessKeyId, secretKey);
		s3_client = new Aws::S3::S3Client(credentials);
	}

	void listBuckets()
	{
      // This probably returns only the first 1000 buckets.
		auto outcome = s3_client->ListBuckets();

		if (outcome.IsSuccess()) {
			std::cout << "Your Amazon S3 buckets:" << std::endl;

			Aws::Vector<Aws::S3::Model::Bucket> bucket_list =
				outcome.GetResult().GetBuckets();

			for (auto const &bucket : bucket_list) {
				// I haven't been able to figure out the string format.  YYYY-MM-DD doesn't work.
				// So I'm using a built-in format for now.
				// string strDate = bucket.GetCreationDate().ToGmtString(Aws::Utils::DateFormat::RFC822);
				std::cout << "  * " << bucket.GetName() << std::endl;
			}
		} else {
			std::cout << "ListBuckets error: "
				<< outcome.GetError().GetExceptionName() << " - "
				<< outcome.GetError().GetMessage() << std::endl;
		}
	}

	void listAllFiles(string bucket_name) {
		//mrr:  this works, but displays all files (keys), without regard to "directories".
      // Actually, it returns only the first 1000.

		cout << "All keys in " << bucket_name << ":" << std::endl;

      int nKeys = 0;

		Aws::S3::Model::ListObjectsV2Request objects_request;
		objects_request.WithBucket(bucket_name);
		auto list_objects_outcome = s3_client->ListObjectsV2(objects_request);
		if (list_objects_outcome.IsSuccess()) {
			Aws::Vector<Aws::S3::Model::Object> object_list =
				list_objects_outcome.GetResult().GetContents();

			for (auto const &s3_object : object_list) {
//				std::cout << "  * " << s3_object.GetKey() << std::endl;
            nKeys++;
			}
         cout << "(Actual keys suppressed.)" << endl;
         cout << nKeys << " keys seen." << endl;
		} else {
			std::cout << "ListObjects error: " <<
				list_objects_outcome.GetError().GetExceptionName() << " " <<
				list_objects_outcome.GetError().GetMessage() << std::endl;
		}
		cout << "-------------" << std::endl;
	}

	void listFiles(TestArgs testArgs) {
		//printf("Enter bucket name to list: ");
		//string bucket_name;
		//getline(cin, bucket_name);

		listAllFiles(testArgs.bucket_name);

		cout << "Listing bucket " << testArgs.bucket_name << " with prefix " << testArgs.prefix << ":" << endl;

		Aws::S3::Model::ListObjectsV2Request objects_request;
		objects_request.WithBucket(testArgs.bucket_name);
		objects_request.WithDelimiter("/");
		string prefix = testArgs.prefix;
		// Apparently the prefix must end in /.
		if (prefix.length() > 0 && prefix[prefix.length() - 1] != '/') {
			prefix += '/';
		}
		objects_request.WithPrefix(prefix);

		// The code below demonstrates how to list all of the "keys" even if multiple 
		// requests are needed.  I believe that by default, only 1000 objects are returned per call.
		bool done = false;
		while (!done) {
			auto list_objects_outcome = s3_client->ListObjectsV2(objects_request);
			if (list_objects_outcome.IsSuccess()) {
				//cout << "List of files and folders with / as delimiter:" << std::endl;
				for (auto const &commonPrefix : list_objects_outcome.GetResult().GetCommonPrefixes()) {
					string folder = commonPrefix.GetPrefix();
					if (folder.find_last_of('/') == folder.length() - 1) {
						folder = folder.substr(0, folder.length() - 1);
					}
					std::cout << "folder " << folder << std::endl;
				}

				Aws::Vector<Aws::S3::Model::Object> object_list =
					list_objects_outcome.GetResult().GetContents();

				for (auto const &s3_object : object_list) {
					std::cout << "file   " << s3_object.GetKey() << std::endl;
				}
			} else {
				std::cout << "ListObjects error: " <<
					list_objects_outcome.GetError().GetExceptionName() << " " <<
					list_objects_outcome.GetError().GetMessage() << std::endl;
			}
			// Figure out whether another request is necessary.
			if (list_objects_outcome.GetResult().GetIsTruncated()) {
				objects_request.SetContinuationToken(list_objects_outcome.GetResult().GetNextContinuationToken());
				cout << " continuing with next request..." << endl;
			} else {
				done = true;
			}
		}
	}

	void populateBucket(TestArgs testArgs) {
		cout << "Uploading to bucket " << testArgs.bucket_name << endl;
		//const char *fileNames[] =
		//{ "myfile1.txt"
		//	, "dir1/subfile1.txt"
		//	, "dir1/subfilesecond.txt"
		//	, NULL };

		std::vector<string> fileNames;
		for (int ifile = 0; ifile < 1200; ifile++) {
			char szbuf[20];
			_itoa_s(ifile, szbuf, 10);
			string filename = "manyfiles/mybigfile";
			filename += szbuf;
			fileNames.push_back(filename);
		}

		for (size_t ifile = 0; ifile < fileNames.size(); ifile++) {
			Aws::S3::Model::PutObjectRequest object_request;
			object_request.WithBucket(testArgs.bucket_name);
			string key = fileNames[ifile];
			cout << "  Uploading " << testArgs.localfile_name << " as " << key << ". ";
			object_request.WithKey(key);
			int mode = std::ios_base::binary | std::ios_base::in;
			auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
				testArgs.localfile_name.c_str(), mode);
			object_request.SetBody(input_data);
			auto outcome = s3_client->PutObject(object_request);
			if (outcome.IsSuccess()) {
				std::cout << "Done!" << std::endl;
			} else {
				std::cout << endl << "PutObject error: " <<
					outcome.GetError().GetExceptionName() << " " <<
					outcome.GetError().GetMessage() << std::endl;
			}
		}
	}

public:
	void dotest(TestArgs testArgs) {
		startup();
		authenticate(testArgs.accessKeyId, testArgs.secretKey);
		if ("list" == testArgs.action) {
			listBuckets();
			listFiles(testArgs);
      } else if("populate" == testArgs.action) {
         populateBucket(testArgs);
      } else if("upload" == testArgs.action) {
		} else {
			cout << "Unrecognized action: " << testArgs.action << endl;
		}
		shutdown();
	}

};

void usage()
{
	printf("Usage: s3vc2017 -key AWSkey -secret AWSsecret -action {list | populate} -bucket bucketName\n");
}

int main(int argc, const char *argv[])
{
	TestArgs testArgs;
	for (int iarg = 1; iarg < argc - 1; iarg++) {
		if (0 == strcmp("-action", argv[iarg])) {
			iarg++;
			testArgs.action = argv[iarg];
		} else if (0 == strcmp("-bucket", argv[iarg])) {
			iarg++;
			testArgs.bucket_name = argv[iarg];
		} else if (0 == strcmp("-key", argv[iarg])) {
			iarg++;
			testArgs.accessKeyId = argv[iarg];
		} else if (0 == strcmp("-secret", argv[iarg])) {
			iarg++;
			testArgs.secretKey = argv[iarg];
		} else if(0==strcmp("-prefix", argv[iarg])) {
			iarg++;
			testArgs.prefix = argv[iarg];
		} else if(0==strcmp("-localfile", argv[iarg])) {
			iarg++;
			testArgs.localfile_name = argv[iarg];
		} else {
			usage();
			return 1;
		}
	}

	cout << "Visual C++ 2017 test app called with action " << testArgs.action << endl;

	S3Test s3test;
	s3test.dotest(testArgs);
	
    return 0;
}

