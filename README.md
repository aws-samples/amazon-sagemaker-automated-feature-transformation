## Automate feature engineering pipelines with Amazon SageMaker

### Overview
In this repository, we provide artifacts that demonstrate how to leverage Amazon SageMaker Data Wrangler, Amazon SageMaker Feature Store, and Amazon SageMaker Pipelines alongside AWS Lambda to automate feature transformation. Before we set up the architecture for automating feature transformations, we first explore the historical dataset with Data Wrangler, define the set of transformations we want to apply, and store the features in Amazon SageMaker Feature Store. This is shown below.

![process-overview.png](https://github.com/aws-samples/amazon-sagemaker-automated-feature-transformation/blob/main/process-overview.png)

### Dataset
To demonstrate feature pipeline automation, we use an example of preparing features for a flight delay prediction model. We use flight delay data from the US Department of [Transportation’s Bureau of Transportation Statistics (BTS)](https://www.transtats.bts.gov/OT_Delay/OT_DelayCause1.asp), which tracks the on-time performance of domestic US flights.
Each record in the flight delay dataset contains information such as:
- Flight date
- Airline details
- Origin and destination airport details
- Scheduled and actual times for takeoff and landing
- Delay details

### Prerequisites
- An [AWS account](https://portal.aws.amazon.com/billing/signup/resume&client_id=signup)
- An Amazon [SageMaker Studio domain](https://docs.aws.amazon.com/sagemaker/latest/dg/onboard-quick-start.html) with the `AmazonSageMakerFeatureStoreAccess` managed policy [attached to the IAM execution role](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_manage-attach-detach.html#add-policies-console)
- An [Amazon S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)

### Walkthrough
For a full walkthrough of automating feature transformation with Amazon SageMaker, see this blog post. It explains more about how to use Amazon SageMaker Data Wrangler for feature transformation, Amazon SageMaker Feature Store for storing those features and Amazon SageMaker Pipelines for automating transformations of all future data.

To follow along, download the Jupyter notebook and python code from this repo. All the instructions are in the blog post and the Jupyter notebook.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

