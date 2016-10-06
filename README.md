# Firebase Keboola Extractor

A Firebase extractor for Keboola Connection is a component extracting data from Firebase REST API endpoints. Written in Node.js with utilization of Babel/ES6/ES7 functionality.

## Configuration

Configuration is very straightforward and deeply described below.

### Sample configuration

    {
      "#apiKey": "some Firebase api key",
      "domain": "domain name in bare form",
      "endpoint": "name of the endpoint"
      "bucketName": "name of the bucket"
      "pageSize": "number which specifies the page size, e.g. 5000",
      "authDomain": "Firebase authDomain, e.g. project-name.firebaseapp.com",
      "databaseURL": "url of the database, https://project-name.firebaseio.com",
      "storageBucket": "firebase storage bucket, e.g. fedger-webhook.appspot.com",
      "batch": "a temporary value, can be set as firstHalf or secondHalf",
      "clientEmail": "admin email from certificate, e.g. name@project-name.iam.gserviceaccount.com",
      "#privateKey": "certificate, the encrypted value will be encrypted again in KBC"
    }
