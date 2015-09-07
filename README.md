# Data-Mining_CS-C415_Project
Project done as a part of Data Mining Course using Hadoop 1.1.2

The aim of this project is to predict the crime on receipt of a distress call to the police, so that the police can accordingly send a response team. The only information available to the police is the caller ID (location of caller).Each city is identified by a unique identification number.
We have used the principle of a Bayesian Classifier.
The descriptions of the mapper and reducer functions are as follows:

####Mapper

The main aim of the mapper function is to calculate ```P(city / crime)```.
Each input row that the mapper receives is for a particular crime. The row also contains the number of instances of the crime in each city ```‘cni’``` as well as the total number of instances of the crime ```‘tc’```. The mapper performs the division ```‘cni/tc’``` for each city.
The output of the mapper is a ```<key,value>``` pair as follows:

*Key*: The city number

*Value*: Contains the name of the crime, P(city / crime), cni, tc all concatenated together as a single text.

####Reducer
The main aim of the reducer is to calculate ```P(crime/city)```.
Once the output of the mapper is received, the total crimes of all types ```‘T’``` is calculated by taking a sum of all ‘tc’. Then the total crimes occurring in a particular city ```’CT’``` is calculated by taking sum of all ‘cni’. Next we use the following equation for deriving the result

```P(city/crime) = [P(crime/city)*P(city)]/p(crime)```

Where

```P(city) = CT/T and P(crime) = tc/T```

The output is a ```<key,value>``` pair as follows:

*Key*: The city number

*Value*: Name of the most likely crime and its probability
