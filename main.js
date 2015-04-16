"use strict";

var moment = require('moment');
var fs = require('fs');
var AWS = require('aws-sdk'); 
var uuid = require('node-uuid');
var pid = uuid.v1();

AWS.config.update({region: 'us-west-2'});
var s3 = new AWS.S3(); 
var db = new AWS.DynamoDB();

var dataProcessed = 0;
var transactionsProcessed = 0;
var errorEvents = 0;

var previousTime = new Date();
setInterval(throughputCalculator, 60000);

var sourceBucketName = 's3stress-source';
var sourceKeyName = 'FourMegFile.txt';
var localFileName = 'downloaded.dat';

var getParams = {Bucket: sourceBucketName, Key: sourceKeyName};

var localFileStream = fs.createWriteStream(localFileName);
var readstream = s3.getObject(getParams).createReadStream();
readstream.pipe(localFileStream);
readstream.on('end',function(){
  setupUploadLoop();
  
});

function setupUploadLoop(){
  fs.readFile(localFileName, function(err, data){
    if (err) { throw err; }    
    else
      writeToS3(data);
  });  
}

function writeToS3(payload){
  var bucketName = 's3stress';
  var keyName = getRandomInt(1, 1000000) + '_' + sourceKeyName;
  var putParams = {Bucket: bucketName, Key: keyName, Body: payload};
  s3.putObject(putParams, function(err, data) {
    if (err){
      console.log(err);
      ++errorEvents;
    }
    else{
      ++transactionsProcessed;
      dataProcessed += payload.length;
      //console.log("Successfully uploaded data to " + bucketName + "/" + keyName);
      setTimeout(function(){
        writeToS3(payload);  
      }, 0);       
    }
  });
}

function throughputCalculator(){
  var timeInterval = new Date() - previousTime;
  var dataThroughput = dataProcessed * 1000/ timeInterval;
  var transactionThroughput = transactionsProcessed  * 1000/ timeInterval;
  var errorRate = errorEvents / (transactionsProcessed + errorEvents) * 100;
  console.log('Data Processed: %d, Transactions Processed: %d, Time Interval: %d, Data Throughput: %d, Transaction Throughput: %d, Error Rate: %d\%',
               dataProcessed, 
               transactionsProcessed,
               timeInterval, 
               dataThroughput, 
               transactionThroughput,
               errorRate);

  var putparams = {
    Item: {
      Pid: {'S': pid},
      Timestamp: {'N': moment().format('x')},
      DataThroughput: {'N': dataThroughput.toString()},
      TransactionThroughput: {'N': transactionThroughput.toString()},
      ErrorRate: {'N': errorRate.toString()},
      Interval: {'N': timeInterval.toString()}
    },
    TableName: 'S3StressResults'
  };

  db.putItem(putparams, function(err, data){
    if (err) console.log(err, err.stack); // an error occurred
    else     
      console.log(data);           // successful response
  });

  dataProcessed = 0;
  transactionsProcessed = 0;
  errorEvents = 0;
  previousTime = new Date();
}

function getRandomInt (low, high) {
    return Math.floor(Math.random() * (high - low) + low);
}