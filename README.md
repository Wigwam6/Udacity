# Project: STEDI Human Balance Analytics

## Brief introduction
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. **Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.**

## Project Solution

### Landing Zone

_**Glue Tables created from  JSON files in S3 bucket:**_
* [customer_landing](./scripts/customer_landing.sql) 
* [accelerometer_landing](./scripts/accelerometer_landing.sql) 
<br>

**Customer Landing Table:**

![customer_landing](/screenshots/customer_landing.png)

**Accelerometer Landing Table:**

![accelerometer_landing](/screenshots/accelerometer_landing.png)

**Row_count Check:**

![customer_accelerometer_landing](/screenshots/customer_accelerometer_landing_rowcount.png)


### Trusted Zone

 
**Customer data:** Customers, who agreed to share their data for research purposes.  
**Accelerometer data:** Accelerometer readings from customers who agreed to share their data for research purposes.
**Step trainer data:** Step Trainer Records for customers who have accelerometer data and have agreed to share their data for research

_**Glue jobs to create trusted data:**_  

* [customer_landing_trusted](./scripts/customer_landing_trusted.py) 
* [accelerometer_landing_trusted](./scripts/accelerometer_landing_trusted.py) 
* [step_trainer_landing_trusted](./scripts/step_trainer_landing_trusted.py) 

**Customer Landing Table:**

![customer_trusted](/screenshots/customer_trusted.png)

**Row_count Check:**

![customer_accelerometer_trusted](/screenshots/customer_accelerometer_trusted_rowcount.png)

### Curated Zone

 
**Customer data:** Only includes customers who have accelerometer data and have agreed to share their data for research.  
**Machine learning curated:** Aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data

_**Glue jobs to create curated data:**_ 

* [customer_trusted](./scripts/customer_trusted_curated.py) 
* [machine_learning_curated](./scripts/machine_learning_curated.py)

![customer_curated](/screenshots/customer_trusted.png)
