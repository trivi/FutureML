# FutureML
Use of machine learning on an encrypted dataset provided by FutureSpace to improve workgroup assignment

# Programs and versions supported
Apache Spark version 2.1.0  
Scala version 2.11.8  
Java version 1.8.0_121  

# Host system
Ubuntu 16.10 64-bit


# How to run
* Place "data.csv" in root folder.  
* Load "DataTransform.scala" in spark-shell to generate the necesary data for training Machine Learning models.  
```:load DataTransform.scala```  
* Load whatever models you like from the "MachineLearningModels" folder to obtain the accuracy
	and a csv file with the absolute number of registers solved in N steps.
