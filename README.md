<div align="center">
  
  <div id="user-content-toc">
    <ul>
      <summary><h1 style="display: inline-block;"> US Accidents Data Analysis 📈</h1></summary>
    </ul>
  </div>
  
  <p>👨‍🔧👷 Data Engineering project Using AWS & Power BI </p>
    <a href="https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents" target="_blank">Data</a>
    ☄️
    <a href="https://github.com/ZaidBenmr/Accidentsdep/issues" target="_blank">Request Feature</a>
</div>
<br>

<div align="center">
      <a href="https://img.shields.io/badge/opencv-%23white.svg?style=for-the-badge&logo=opencv&logoColor=white">
      </a>
      <img src="https://img.shields.io/github/stars/hamagistral/DataEngineers-Glassdoor?color=blue&style=social"/>
</div>

## 📝 Table of Contents

1. [ Project Overview ](#introduction)
2. [ Project Architecture ](#arch)
3. [ Dashboard ](#dashboard)
4. [ Key insights ](#features)
5. [ Data Model ](#datamodel)
6. [ Credits ](#refs)
7. [ Contact ](#contact)
<hr>



<a name="introduction"></a>
## 🔬 Project Overview :

### 💾 Dataset : 
This dataset provides a comprehensive overview of car accidents across 49 states in the USA. The data spans from February 2016 to March 2023 and was collected using multiple APIs that stream traffic incident data. These APIs aggregate traffic information from various sources, including the US and state departments of transportation, law enforcement agencies, traffic cameras, and traffic sensors within the road networks. Currently, the dataset comprises approximately 7.7 million accident records.
The dataset includes the following columns:

| **Column** | **Description** |
| :--------------- |:---------------| 
| **ID** |  This is a unique identifier of the accident record. |  
| **Severity** | Shows the severity of the accident, a number between 1 and 4, where 1 indicates the least impact on traffic. |
| **Start_Time**   |  Shows start time of the accident in local time zone.  |
| **End_Time**   |  Shows end time of the accident in local time zone. End time here refers to when the impact of accident on  |
| **Start_Lat**   |  Shows latitude in GPS coordinate of the start point.  |
| **Start_Lng**   |   Shows longitude in GPS coordinate of the start point.  |
| **End_Lat**   |  Shows latitude in GPS coordinate of the end point.  |
| **End_Lng**   |  Shows longitude in GPS coordinate of the end point.  |
| **Distance(mi)**   |  The length of the road extent affected by the accident in miles.  |
| **Street**   |  Shows the street name in address field.  |
| **City**   |  Shows the city in address field.  |
| **County**   |  Shows the county in address field.  |
| **State**   |  Shows the state in address field.  |
| **Zipcode**   |  Shows the zipcode in address field.  |
| **Country**   |  Shows the country in address field.  |
| **Weather_Timestamp**   |  Shows the time-stamp of weather observation record (in local time).  |
| **Temperature(F)**   |  Shows the temperature(in Fahrenheit).  |
| **Wind_Chill(F)**   |  Shows the wind chill (in Fahrenheit).  |
| **Humidity(%)**   |  Shows the humidity (in percentage).  |
| **Pressure(in)**   |  Shows the air pressure (in inches).  |
| **Visibility(mi)**   |  Shows visibility (in miles).  |
| **Weather_Condition**   |  Shows the weather condition (rain, snow, thunderstorm, fog, etc.)  |
| **Crossing**   |  A POI annotation which indicates presence of crossing in a nearby location.  |
| **Railway**   |  A POI annotation which indicates presence of railway in a nearby location.  |
| **Traffic_Calming**   |  A POI annotation which indicates presence of traffic_calming in a nearby location.  |


### 🎯 Goal :

This is an end-to-end Data Engineering project where I designed and implemented a star schema in Amazon Redshift for analyzing US accident data. I created an ETL data pipeline using AWS Glue to extract accident data, transform it, and load it into dimension and fact tables in Amazon Redshift.

The star schema includes tables for date, time, location, weather, and accident facts, allowing for efficient querying and analysis. I utilized AWS Glue and Redshift to preprocess and store the data in a structured format.

The Power BI dashboard includes filters for city, date range, and severity type, providing dynamic visualizations such as total number of accidents, total number of states affected, accidents per day/month, weather conditions, and severity heatmaps. Additional measures like the state with the most accidents and the most common weather condition during accidents offer deeper insights.

This project showcases my skills in data modeling, ETL pipeline creation, cloud-based data processing, and interactive dashboard development.


### 🧭 Steps :

In our way to implemente this project, we've passed with the following steps :

#### 1- Store the US accidents CSV file into an AWS S3 raw data bucket.
#### 2- Use a AWS Glue PySpark script to clean the data and transform the file type to Parquet.
#### 3- Crawl the cleaned and transformed data using AWS Glue to create metadata and make it accessible for further processing.
#### 4- Use another PySpark script to load the data from the Parquet files into Amazon Redshift.
#### 5- Implement a star schema in Redshift, including dimension tables for date, time, location, and weather, and a fact table for accidents.
#### 6- Optimize the star schema for efficient querying and analysis.
#### 7- Create a Power BI dashboard to visualize the data. Include filters for city, date range, and severity type.

<a name="arch"></a>
## 📝 Project Architecture
<div align="center">
  <img src="https://github.com/ZaidBenmr/Accidentsdep/blob/main/images/architecture.PNG" alt="Banner" width="1200" height="350">
</div>

### 🛠️ Technologies Used
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![PySpark](https://img.shields.io/badge/PySpark-%23E25A1C.svg?style=for-the-badge&logo=Apache%20Spark&logoColor=white)
![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white)
![AWS](https://img.shields.io/badge/Amazon%20Web%20Services-232F3E.svg?style=for-the-badge&logo=Amazon-Web-Services&logoColor=white)
![Power Bi](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

<a name="dashboard"></a>
## 📊 Dashboard
<div align="center">
  <img src="https://github.com/ZaidBenmr/Accidentsdep/blob/main/images/US Accidents dashboard 1.PNG" alt="Banner">
</div>
<div align="center">
  <img src="https://github.com/ZaidBenmr/Accidentsdep/blob/main/images/US Accidents dashboard 2.PNG" alt="Banner">
</div>

<a name="features"></a>
## 🕵️ Key Insights

- 🚗 **Total Accidents and Geographic Spread**
  - The dashboard highlights a total of 7 Millions accidents recorded, indicating a significant impact nationwide. Analysis reveals that accidents are spread across 49 states, underscoring the widespread nature of these incidents.

- 🏙️ **City with the Highest Accident Frequency**
  - Miami stands out as the city with the highest number of accidents, emphasizing localized challenges and the need for targeted safety measures in urban areas.

- 🌡️ **Severity Analysis**
  - A notable 273k of accidents are classified as severe, highlighting critical areas where safety protocols and emergency response strategies are crucial.

- 📊 **Temporal Patterns and Environmental Influences**
  - Accidents peak on Friday days, suggesting potential causal factors and opportunities for targeted interventions to reduce accident rates.
  - Seasonal variations are evident in the data, with December months showing higher accident rates, indicating seasonal influences on accident frequency.

- 🔥 **Severity Heatmap and Environmental Factors**
  - The severity heatmap identifies hotspots where accidents are most severe, guiding resource allocation and emergency response efforts to these critical areas.
  - Analysis of accidents based on temperature and pressure reveals correlations, with 18% occurring at temperature 73 and 10.48% at pressure 29.96, providing insights into environmental conditions that impact accident severity.

- 📈 **Average Severity Metrics**
  - Average severity metrics highlight variations in accident outcomes based on temperature and wind speed, providing insights into the relationship between environmental conditions and accident severity.


<a name="datamodel"></a>
## 🛢️ Data Model : 
<div align="center">
  <img src="https://github.com/ZaidBenmr/Accidentsdep/blob/main/images/data model.PNG" alt="Banner">
</div>

<a name="refs"></a>
## 📋 Credits

- This Project is inspired by :
  - The video of the [YouTube Channel Darshil Parmar](https://www.youtube.com/watch?v=qBocgdMGEWs&list=PLBJe2dFI4sgvavQzL2Hm5CsnoIWHY5fI3)
  - The github profile of [Hamagistral](https://github.com/Hamagistral/VCT2023-Kafka-AWS)

<a name="contact"></a>
## 📨 Contact Me

[LinkedIn](https://www.linkedin.com/in/zaid-benamar/) •
[Gmail](zaid.benmr@gmail.com)
