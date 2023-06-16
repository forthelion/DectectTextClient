using System;
using System.IO;
using System.Threading.Tasks;
using Amazon;
using Amazon.Rekognition;
using Amazon.Rekognition.Model;
using Amazon.S3;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using Amazon.Lambda.Core;
using Amazon.EventBridge;
using Amazon.EventBridge.Model;
using System.Collections.Generic;
using Amazon.Lambda.S3Events;
using static System.Net.Mime.MediaTypeNames;
using Image = Amazon.Rekognition.Model.Image;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace PlateReaderFunction
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }
        IAmazonEventBridge EventBridgeClient { get; set; }
        string EventSource { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
            EventBridgeClient = new AmazonEventBridgeClient();
            EventSource = "NotCalifornia";
        }

        /// <summary>
        /// Constructs an instance with preconfigured clients. This can be used for testing outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        /// <param name="eventBridgeClient"></param>
        /// <param name="eventSource"></param>
        public Function(IAmazonS3 s3Client, IAmazonEventBridge eventBridgeClient, string eventSource)
        {
            this.S3Client = s3Client;
            this.EventBridgeClient = eventBridgeClient;
            this.EventSource = eventSource;
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            var bucketName = s3Event.Bucket.Name;
            var objectKey = s3Event.Object.Key;
            string plateData = null;
            string plateDataJson = null;
            var sqsClient = new AmazonSQSClient();

            Console.WriteLine("Getting object and detecting text...");

            var response = await S3Client.GetObjectAsync(bucketName, objectKey);
            {
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    string metadatalocation = response.Metadata["your-metadata-key1"];
                    string metadadate = response.Metadata["your-metadata-key2"];
                    string metadatatype_of_violation = response.Metadata["your-metadata-key3"];
                    

                    Console.WriteLine("Detecting text from image...");
                    plateData = await DetectTextAsync(objectKey,bucketName).ConfigureAwait(false);

                    var data = new
                    {
                        location = metadatalocation,
                        date = metadadate,
                        type_of_violation = metadatatype_of_violation,
                        plateInformation = plateData
                    };


                    plateDataJson = JsonConvert.SerializeObject(data);

                    if (plateData.ToUpper().Contains("california".ToUpper()))
                    {
                        Console.WriteLine("Converting to JSON...");


                        string qUrl = "https://sqs.us-east-1.amazonaws.com/392106205903/downwardQueue";
                        Console.WriteLine("Sending message to the queue...");
                        await SendMessage(sqsClient, qUrl, plateDataJson);
                        Console.WriteLine("Message sent to the queue.");
                    }
                    else
                    {
                        Console.WriteLine("Publishing event to the EventBridge...");
                        await PublishEvent("TextMessage", plateData);
                        Console.WriteLine("Event published to the EventBridge.");
                    }
                }
            }

            // Detect text
            static async Task<string> DetectTextAsync(string photo,string bucket)
            {
                using (var rekognitionClient = new AmazonRekognitionClient())
                {
                    DetectTextRequest detectTextRequest = new DetectTextRequest()
                    {
                        Image = new Image()
                        {
                            S3Object = new S3Object()
                            {
                                Name = photo,
                                Bucket = bucket
                            }
                        }
                    };
                try
                { 
                    DetectTextResponse detectTextResponse = rekognitionClient.DetectTextAsync(detectTextRequest);
                 
                    var detectedText = string.Empty;

                    foreach (TextDetection text in detectTextResponse.TextDetections)
                    {
                        if (text.Type == TextTypes.LINE)
                        {
                            detectedText += text.DetectedText + " ";
                        }
                    }

                    return detectedText.Trim();
                }
                catch (Exception e)
                {
                        Console.WriteLine(e.Message);
                }

                }

            static async Task SendMessage(IAmazonSQS sqsClient, string qUrl, string messageBody)
            {
                Console.WriteLine(messageBody);
                SendMessageResponse responseSendMsg = await sqsClient.SendMessageAsync(qUrl, messageBody);
                Console.WriteLine($"Message added to queue: {qUrl}");
                Console.WriteLine($"HTTP Status Code: {responseSendMsg.HttpStatusCode}");
            }

            async Task<bool> PublishEvent(string detailType, string jsonDetail)
            {
                try
                {
                    var request = new PutEventsRequest
                    {
                        Entries = new List<PutEventsRequestEntry>
                        {
                            new PutEventsRequestEntry
                            {
                                Source = EventSource,
                                DetailType = detailType,
                                Detail = jsonDetail
                            }
                        }
                    };

                    var response = await EventBridgeClient.PutEventsAsync(request);
                    return response.HttpStatusCode == System.Net.HttpStatusCode.OK;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error sending event: " + ex.Message);
                    return false;
                }
            }
        }
    }
}
