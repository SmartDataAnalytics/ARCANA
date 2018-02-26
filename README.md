# ARCANA
> L**ar**ge S**c**ale Qu**a**lity Assessme**n**t For Potential Dual Use with Sp**a**rk

Over the last years, the Semantic Web has been growing from a mere idea up to the point of at least 10,000 datasets available online.
Sentiment analysis on structured data has attracted much attention recently. This amount of Linked Data offers huge potential and can be harnessed to receive the sentiment tendency towards these topics. However, since none can invest an infinite amount of time to read through these data, an automated profiling approach is necessary. Nevertheless, most existing solutions are limited on centralized environments only.
In this thesis, we go one step further and develop a novel approach for sentiment learning by using Spark framework.
First part of this project is to build an intelligent algorithm to determine the resources that may be used as a harmful knowledge. There are a lot of information offered in knowledge bases, some of it could be used in a malicious way, and some are not. However, this part will depend on sentimental analysis and machine learning using large data scale RDF and Spark framework..
Furthermore, we are going to build an intelligent algorithm to decide if a natural language question could be used for harmful purposes. There are a lot of questions online used to acquire malicious information. These questions could lead to dangerous actions. Therefore, we will focus on sentiment analysis and deep learning to achieve that goal and make usage of Spark framework.

Usage
----------

```
git clone https://github.com/SmartDataAnalytics/ARCANA.git
cd SANSA-Template-Maven-Spark

mvn clean package
````
Database
----------

```
The application make use of MongoDB (NoSQL database), so it is important to have it installed and running.
To install it, refer to https://docs.mongodb.com/manual/administration/install-on-linux/
````

Remarks
----------
If you wish to change something in the project, be sure to use Scala-2.11. 
For example (Using Scala IDE):
````
Project >> Properties >> Scala Compiler >> Scala Installation : Latest 2.11 bundle (dynamic) 
````
