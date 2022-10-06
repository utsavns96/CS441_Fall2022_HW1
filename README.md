# CS441_Fall2022_HW1
## Utsav Sharma
### UIN: 665894994
### NetID: usharm4@uic.edu

Repo for the MapReduce homework-1 for CS411-Fall2022

---

AWS EMR Deployment video link: https://youtu.be/OgogHoK0vOI

---

## Running the project
1) Download the repo from git.
2) Navigate to the LogGenerator directory.
3) From the terminal, run `sbt clean compile`.
4) From the terminal, run `sbt "run <input-path> <output-path>"` without the angular braces.
5) To test, run `sbt test`.
6) To create a .jar file, run the command `sbt assembly`. The resulting jar will be placed in __LogFileGenerator\target\scala-3.0.2__
---

## Requirements:

In this homework, we have to use Map/Reduce to analyse logs and implement 4 major functionalities:
1) Compute a spreadsheet or an CSV file that shows the distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types. (This is implemented by **DistributionCSV.scala**)
2) Compute time intervals sorted in the descending order that contained most log messages of the type ERROR with injected regex pattern string instances. (This is done via) **DescendingOrderofError.scala**
3) For each message type you will produce the number of the generated log messages. (This is done through **NumberofMessages.scala**)
4) Produce the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern. (This is implemented via **LongestString.scala**)

Other Requirements:
1) The output files should be in the format of .csv.
2) 5 or more scalatests should be implemented.
3) Logging used for all programs.
4) Configurable input and output paths for the map/reduce programs.
5) Configurable control variables (Time intervals, Regular Expressions, etc)
6) Compileable through sbt
7) Deployed on AWS EMR

---

## Technical Design

We will take a look at the detailed description of how each of these pieces of code work below. Line by line comments explaining every step are also added to the source code in this git repo.:

1) ### RunJobs.scala
	This is the main method that we call to run all our programs. It has been added to simplify the usage of the map/reduce programs, so that we don't have to call the 4 codes individually. It takes our input and output path, and uses them to call each of the 4 files one by one. It passes the input and output paths to the 'main' classes of the files in their arguments.

2) ### DistributionCSV.scala

    This takes our input of the path of the input and output directories and runs our log file through the mapper method _map()_
    In this, we first fetch the regular expression patterns that we need to match to from the `application.conf` file under the headers _functionalityconfigs.mapreducetocsv.FindOccurrenceOf_ and _randomLogGenerator.Pattern_ , and use the java.util.regex.Pattern library to perform our pattern matching using the _matcher()_ method.
    We use _.find()_ to check if we have a successful match in the input string, and if that holds true for both the injected pattern and the user defined pattern, we proceed further.
    The next step is to calculate the time interval based on which we will be grouping our error messages. To do this, we first fetch the time interval defined in the `application.conf` file under _functionalityconfigs.mapreducetocsv.TimeInterval_, and then check if the modulus of the minute of the log message (substring(3,5)) and the time interval.
    If it is 0, it means that we are at a time interval, and that we can set the key of the map to the hour:minute(from the log message):00 + the matched pattern, which we get using the _.group()_ method that returns the substring matched by the _.matcher()_.
    If the above condition is not met, we check if the minute of the log message is between the preceding time interval minute and the next time interval minute, in which case we set the key of the map to the hour(from the log message) + the lower time interval minute + the matched string.
    If both these conditions are not met, we set the key to the higher of the time interval minute + the matched pattern.
    In all cases, the value for the map is set to 1, since we are counting the number of occurrences at this step. 

    In the reducer _reduce()_, we simply add up all the inputs from the map. The output of this is a binary tab separated file by default, so we change our separator to ',' in the job configuration by setting `conf.set("mapred.textoutputformat.separator", ",")` in the method _mapred()_, which calls the map and reduce through configuring and running a job. To make things easier for us, the output file name is dynamically generated at runtime, and it appends the configuration _functionalityconfigs.mapreducetocsv.OutputPath_ from the application.conf file, + the current date and time in "dd-MM-yyyy-hh-mm-ss" format. This makes it so that the output directory path generated every time is unique, and the user does not have to delete previous folders for future runs. Lastly, we call a specialized helper function in `HelperUtils` called `ExtensionRenamer.changeExt` with the output path and the job name as the arguments. This renames the file from **part-00000** to the name of the job, and changes the extension to **.csv**.
	<br><br>
	
	**Sample Output:**

    | Key | Value |
    |-----|-------|
    | 14:35:00 DEBUG | 4 |
    | 14:35:00 ERROR | 1 |
    | 14:35:00 INFO |58 |
    | 14:35:00 WARN	|23 |
    | 14:36:00 DEBUG	|52 |
    | 14:36:00 ERROR	|3 |
    | 14:36:00 INFO	|360 |
    | 14:36:00 WARN	|82 |
    | 14:37:00 DEBUG	|46 |
    | 14:37:00 ERROR	|4 |
	| 14:37:00 INFO	| 360 |
	| 14:37:00 WARN	| 79 |
    
    * The headers are not present in the actual output - they need to be added because markdown doesn't support tables without headers.

3) ### DescendingOrderofError.scala

	Like the above code, this takes the same inputs and runs them through a map/reduce job. Here, the first mapper _map()_ takes the injected regex pattern and the user defined regex pattern from the `application.conf` file under the headers _functionalityconfigs.descendingorder.FindOccurrenceOf_ and _randomLogGenerator.Pattern_, and runs the input file through the _.matcher()_ to find the log messages that satisfy our regex constrains. When a hit is found through the two _.find()_, it takes the minute from the log message (substring(3,5)) and performs a modulus with the time interval that we have specified in the 'application.conf' file under _functionalityconfigs.descendingorder.TimeInterval_. If the modulus returns 0, it means that we are at a time interval minute, and the key is set to that time interval + the matched string (ERROR). If the modulus is between a time interval and the next time interval, we set the key to the lower time interval + the matched log message type (ERROR). Else, we set the key to the higher time interval + the matched string (ERROR). In each case, the value is set to 1.
	Even though the we only need to find the log messages of type **ERROR**, the _map()_ takes this configuration from _functionalityconfigs.descendingorder.FindOccurrenceOf_ in the `application.conf` file, which means that we can repurpose this code to find the descending order of any other log message (**INFO**/**DEBUG**/**WARN**) by simply changing the value of _**FindOccurrenceOf**_ in `application.conf`.
	After this step, we run them through the reducer _reduce()_ where we add up all the outputs of the _map()_ to produce an unsorted file with all the log messages of ERROR that contained the two regex patterns we defined. This is stored in the folder with the name DescendingOrderofError + the current date and time in "dd-MM-yyyy-hh-mm-ss" format \ unsortedoutput. EVen though we call this unsorted, in reality it is sorted according to the ascending order of the keys, which is not what we want.
	Then, we run this unsorted file through a second map/reduce job, where we sort the output in descending order. By default, the mapper will sort its outputs in ascending order of the key, so to sort this file on the descending order of the number of log messages (the value in the unsorted file), we multiply the value by -1, and swap the key and value such that the count of messages is now the key, and the string with the [time interval log message type] is the value. Since negative numbers are smaller when their absolute value is larger, the mapper _map()_ from the class _SortMap_ sorts them in ascending order according to it, but in reality is sorting them in the descending order that we need.
	Now that we have the output of this map, we need to multiple our keys by -1 again to make them positive, and then swap them back to their original places. The output for this second map/reduce is placed in the folder DescendingOrderofError + the current date and time in "dd-MM-yyyy-hh-mm-ss" format \ finaloutput.
	The two map/reduce jobs are run one after the other by simply calling them one after the other.
	This however, does satisfy all our requirements, since these files are binary tab separated. To fix this, we set the separator to ',' in the map/reduce job configurations and run them through the specialized helper function in `HelperUtils` called `ExtensionRenamer.changeExt`, which we call twice - once for the unsorted output and the second time for the sorted output. This changes the extension of both files to **.csv**, which means that we have now met all requirements for this functionality.
	<br><br>
	
	**Sample Unsorted Output:**

    | Key | Value |
    |-----|-------|
    | 14:35:00->ERROR | 1 |
	| 14:36:00->ERROR | 3 |
	| 14:37:00->ERROR | 4 |
	| 14:38:00->ERROR | 7 |
	| 14:39:00->ERROR | 5 |
	| 14:40:00->ERROR | 7 |
	| 14:41:00->ERROR | 9 |
	| 14:42:00->ERROR | 2 |
	| 14:43:00->ERROR | 8 |
	| 14:44:00->ERROR | 3 |
	| 14:45:00->ERROR | 3 |
	| 14:46:00->ERROR | 1 |
	
	<br><br>
	
	**Sample Sorted Output:**

    | Key | Value |
    |-----|-------|
    | 14:41:00->ERROR| 9 |
	| 14:43:00->ERROR| 8 |
	| 14:40:00->ERROR| 7 |
	| 14:38:00->ERROR| 7 |
	| 14:39:00->ERROR| 5 |
	| 14:37:00->ERROR| 4 |
	| 14:45:00->ERROR| 3 |
	| 14:44:00->ERROR| 3 |
	| 14:36:00->ERROR| 3 |
	| 14:42:00->ERROR| 2 |
	| 14:46:00->ERROR| 1 |
	| 14:35:00->ERROR| 1 |
    
    * The headers are not present in the actual output - they need to be added because markdown doesn't support tables without headers.
	
4) ### NumberofMessages.scala
	Here, we take the path to the input log file and the output directory as arguments, and run the file through a simple map-reduce. The mapper method _map()_ takes the file and matches the strings with the log message type regular expression defined in `application.conf` under _functionalityconfigs.NumberofMsg.FindOccurrenceOf_.
	If we find a match from the _.find()_, we get the matched value using the _group()_ method, and set that to the key. The value is set to 1. This is because we want to calculate the occurence of each message type, and setting the value of to 1 will allow us to add them all up in the reducer.
	In the reducer _reduce()_, we sum up all the values to get the total number of occurences of each of the 4 log message types. As with the above 2 functionalities, the output is a binary tab separated file named **part-00000**, so we set `conf.set("mapred.textoutputformat.separator", ",")` and also run the file through `ExtensionRenamer.changeExt` in `HelperUtils` to change the name to the job name and the extension to .csv. We also dynamically generate the output subdirectory like in the above programs, by taking the name of the directory from _functionalityconfigs.NumberofMsg.OutputPath_ + the current date and time in "dd-MM-yyyy-hh-mm-ss" format.
	<br><br>
	**Sample Output:**

    | Key | Value |
    |-----|-------|
    | DEBUG | 70491 |
    | ERROR | 163 |
    | INFO | 84523 |
    | WARN	| 3203 |

    * The headers are not present in the actual output - they need to be added because markdown doesn't support tables without headers.
	
5) ### LongestString.scala
	Like the other 3 functionalities described above, we start with the input path and output path as the input to our map/reduce. Here, we run the input file through the mapper _map()_, matching the input log string to the regular expressions that we fetch from `application.conf`, in the fields _functionalityconfigs.longeststring.FindOccurrenceOf_ and _randomLogGenerator.Pattern_. Once we get a match from our _.find()., the mapper creates a simple map of the log message type and the length of the injected regex string that we match.
	This is then passed onto the reducer where we find the maximum of the values we get from the mapper, and finally print the largest to our output file. This file, like before, is a tab separated binary file which we first turn into comma separated with the line `conf.set("mapred.textoutputformat.separator", ",")`, and then after running the job, call `ExtensionRenamer.changeExt` from `HelperUtils` to change the filename to the job name and the extension to .csv.
	<br><br>
	**Sample Output:**

    | Key | Value |
    |-----|-------|
    | DEBUG | 30 |
    | ERROR | 21 |
    | INFO | 18 |
    | WARN	|21 |

    * The headers are not present in the actual output - they need to be added because markdown doesn't support tables without headers.
	


---

## Test Cases

| Case No. | Test Name                 | Test Steps                                                                                                                               | Expected Result                                            | Actual Result                                              | Pass/Fail |
|----------|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------|------------------------------------------------------------|-----------|
| 1 | Check if application.conf file is present | (1.) Load config file application.conf <br> (2.) Assert that the file is present | The file is present. | The file is present in the /resources directory. | Pass |
| 2 | Unit test for config load | (1.) Load config file application.conf <br> (2.) Load the functionality configs <br> (3.) Assert that the config variables are not empty | The file is loaded and the config variables are not empty. | The file is loaded and the config variables are not empty. | Pass |
| 3 | Unit test for OutputPath  | (1.) Load the config file <br> (2.) load each of the output paths from their respective headers <br> (3.) Assert that the 4 output paths are not empty | The output paths for all the 4 programs are present.       | The output paths for all the 4 programs are present.       | Pass |
| 4 |  Unit test for user defined regex positive | (1.) Load config file application.conf <br> (2.) Load the user defined regex pattern from functionality configs <br> (3.) load the sample log string with the regex to be found <br>(4.)  Assert that the string matches the regex pattern | The regex pattern should match | The regex pattern matches | Pass |
| 5 | Unit test for user defined regex negative | (1.) Load config file application.conf <br> (2.) Load the user defined regex pattern from functionality configs <br> (3.) load the sample log string with the regex to be found <br>(4.)  Assert that the string should not match the regex pattern | The regex pattern should not match | The regex pattern matches | Pass |
| 6 | Unit test for injected regex negative |  (1.) Load config file application.conf <br> (2.) Load the injected regex pattern from functionality configs <br> (3.) load the sample log string with the regex to be found <br>(4.)  Assert that the string matches the regex pattern | The regex pattern should match | The regex pattern matches | Pass |
| 7 | Unit test for injected regex negative | (1.) Load config file application.conf <br> (2.) Load the injected regex pattern from functionality configs <br> (3.) load the sample log string with the regex to be found <br>(4.)  Assert that the string should not match the regex pattern | The regex pattern should not match | The regex pattern matches | Pass |