# Big data Python + Spark project

## Table of contents
- [Task content](#task-content)
- [Project description](#project-description)
- [Technologies and dependencies](#technologies-and-dependencies)
- [Requirements](#requirements)
- [Build instruction](#build-instruction)
- [Status](#status)
- [Contact](#contact)

## Task content

###  Given datasets
####  Mobile App click stream projection

Schema:
* userId: String
* eventId: String
* eventTime: Timestamp
* eventType: String
* attributes: Map[String, String]

There could be events of the following types that form a user engagement session:

* app_open 
* search_product 
* view_product_details 
* purchase 
* app_close

Events of app_open type may contain the attributes relevant to the marketing analysis:
* campaign_id
* channel_id

Events of purchase type contain purchase_id attribute.

####  Purchases projection

Schema:
* purchaseId: String
* purchaseTime: Timestamp
* billingCost: Double
* isConfirmed: Boolean

### Tasks & Requirements

####  Tasks #1 - Build Purchases Attribution Projection

Target schema:

* purchaseId: String
* purchaseTime: Timestamp
* billingCost: Double
* isConfirmed: Boolean
* sessionId: String // a session starts with app_open event and finishes with app_close
* campaignId: String  // derived from app_open#attributes#campaign_id
* channelIid: String    // derived from app_open#attributes#channel_id

####  Tasks #2 - Calculate Marketing Campaigns And Channels Statistics

Task #2.1. Top Campaigns:
* Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases) <br />

Task #2.2. Channels engagement performance:
* Most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)  with the App in each campaign <br />

## Project description
Project consists of the following directories: <br />
* bigdata-input-generator <br />
  This directory content is necessary to generate sample input data for project analyze marketing data purposes
* configs <br />
  It's a directory to store any external configuration parameters required by spark jobs in JSON format
* dependencies <br />
  Here you can find some general project dependencies (global spark configuration, logger setup etc.)
* jobs <br />
  This directory consists of spark app created jobs
* tests <br />
  Here you can find some spark app tests
  
## Technologies and dependencies
* Python 3.8
* PySpark 3.1.2

## Requirements
* Git
* Python 3.8 (or higher) and pip3 (package-management system)

## Build instruction
To run project, follow these steps: <br />
1. Open terminal and clone the project from github repository:
```
$ git clone https://github.com/mkrolczyk12/python-spark.git
```
```
$ cd <project_cloned_folder>
```
where <project_cloned_folder> is a path to project root directory <br />

2. Create and activate virtualenv: <br />
* If no virtualenv package installed, run:
```
$ python3 -m pip install --upgrade pip
$ pip3 install virtualenv
```   
* Then
```
$ python3 -m venv ENV_NAME
```
where 'ENV_NAME' is the name of env
* Activate virtualenv
```
$ source ./ENV_NAME/bin/activate
```

3. Generate sample input data for your project:
* Install the required python packages and run the main script from the bigdata-input-generator directory to generate 
the datasets
```
(ENV_NAME)$ pip3 install -r ./bigdata-input-generator/requirements.txt
(ENV_NAME)$ python3 ./bigdata-input-generator/main.py
```

4. Install project required dependencies:
```
(ENV_NAME)$ pip3 install -r ./requirements.txt
```

5. Check if everything works by running tests
```
(ENV_NAME)$ python3 -m tests.test_task1
(ENV_NAME)$ python3 -m tests.test_task2
```
5. Run main script
```
(ENV_NAME)$ python3 -m jobs.main
```
The app should be ready to use.

## Status

_completed_


## Contact

Created by @mkrolczyk12 - feel free to contact me!

- E-mail: m.krolczyk66@gmail.com
