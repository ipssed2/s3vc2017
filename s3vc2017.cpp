// s3vc2017.cpp:  Application to test the AWS C++ library, for S3.
// Mark Riordan  2017-07-19

#include "stdafx.h"
#include <iostream>
#include <fstream>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Bucket.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
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
   string remote_dir;
   string remote_key;
	string localfile_name;
   string localdir_name;
   int count = 0;
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
      if(testArgs.remote_key.length() == 0 || testArgs.count == 0 || testArgs.localfile_name.length() == 0) {
         cerr << "You must specify: " << endl;
         cerr << "  -remotekey to give the filename prefix" << endl;
         cerr << "  -count for # of copies to upload" << endl;
         cerr << "  -localfile for the file to upload" << endl;
      } else {
         cout << "Uploading to bucket " << testArgs.bucket_name << endl;

         std::vector<string> fileNames;
         for(int ifile = 0; ifile < testArgs.count; ifile++) {
            char szbuf[20];
            _itoa_s(ifile+1, szbuf, 10);
            string filename = testArgs.remote_key;
            filename += szbuf;
            filename += ".bin";
            fileNames.push_back(filename);
         }

         for(size_t ifile = 0; ifile < fileNames.size(); ifile++) {
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
            if(outcome.IsSuccess()) {
               std::cout << "Done!" << std::endl;
            } else {
               std::cout << endl << "PutObject error: " <<
                  outcome.GetError().GetExceptionName() << " " <<
                  outcome.GetError().GetMessage() << std::endl;
            }
         }
      }
	}

   void upload(TestArgs testArgs) {
      cout << "Uploading " << testArgs.localfile_name << " to bucket " << testArgs.bucket_name << " dir " << testArgs.remote_dir << endl;

   }

   void download(TestArgs testArgs) {
      cout << "Downloading " << testArgs.remote_key << " from bucket " << testArgs.bucket_name << " to " << testArgs.localdir_name << endl;

      Aws::S3::Model::GetObjectRequest object_request;
      object_request.WithBucket(testArgs.bucket_name).WithKey(testArgs.remote_key);

      auto get_object_outcome = s3_client->GetObject(object_request);

      if(get_object_outcome.IsSuccess()) {
         Aws::OFStream local_file;
         string localFilename = testArgs.localdir_name + "\\";
         size_t idx = testArgs.remote_key.find_last_of('/');
         if(Aws::String::npos == idx) {
            localFilename += testArgs.remote_key;
         } else {
            localFilename += testArgs.remote_key.substr(1+idx);
         }
         local_file.open(localFilename.c_str(), std::ios::out | std::ios::binary);
         local_file << get_object_outcome.GetResult().GetBody().rdbuf();
         std::cout << "Done downloading to " << localFilename << std::endl;
      } else {
         std::cout << "GetObject error: " <<
            get_object_outcome.GetError().GetExceptionName() << " " <<
            get_object_outcome.GetError().GetMessage() << std::endl;
      }
   }

   void deleteKey(TestArgs testArgs) {
      Aws::S3::Model::DeleteObjectRequest object_request;
      object_request.WithBucket(testArgs.bucket_name).WithKey(testArgs.remote_key);

      auto delete_object_outcome = s3_client->DeleteObject(object_request);

      if(delete_object_outcome.IsSuccess()) {
         // Unfortunately, IsSuccess() is true even if the key did not exist.
         // See https://stackoverflow.com/questions/30697746/why-does-s3-deleteobject-not-fail-when-the-specified-key-doesnt-exist.
         std::cout << "Deleted " << testArgs.remote_key << " OK." << std::endl;
      } else {
         std::cout << "DeleteObject error: " <<
            delete_object_outcome.GetError().GetExceptionName() << " " <<
            delete_object_outcome.GetError().GetMessage() << std::endl;
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
         upload(testArgs);
      } else if("download" == testArgs.action) {
         download(testArgs);
      } else if("deletekey" == testArgs.action) {
         deleteKey(testArgs);
		} else {
			cout << "Unrecognized action: " << testArgs.action << endl;
		}
		shutdown();
	}

};

void usage()
{
   printf("Usage: s3vc2017 -accesskey accessKey -secret AWSsecret\n");
   printf("  -action {list | populate | download | upload | deletekey}\n");
   printf("  -bucket bucketName [-localfile localFilename] [-remotedir keyName]\n");
   printf("  [-localdir localDirName] [-remotekey remoteKeyName] [-count count]\n");
   printf("  [-newname newname]\n");
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
		} else if (0 == strcmp("-accesskey", argv[iarg])) {
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
      } else if(0==strcmp("-localdir", argv[iarg])) {
         iarg++;
         testArgs.localdir_name = argv[iarg];
      } else if(0==strcmp("-remotedir", argv[iarg])) {
         iarg++;
         testArgs.remote_dir = argv[iarg];
      } else if(0==strcmp("-remotekey", argv[iarg])) {
         iarg++;
         testArgs.remote_key = argv[iarg];
      } else if(0==strcmp("-count", argv[iarg])) {
         iarg++;
         testArgs.count = atoi(argv[iarg]);
      } else {
         cout << "Unrecognized argument: " << argv[iarg] << endl;
			usage();
			return 1;
		}
	}

	cout << "Visual C++ 2017 test app called with action " << testArgs.action << endl;

	S3Test s3test;
	s3test.dotest(testArgs);
	
    return 0;
}

