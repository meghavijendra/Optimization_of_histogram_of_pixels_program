# PROJECT DESCRIPTION

The purpose of this project is to improve the performance of the program that creates histograms of pixels developed by using a <b>combiner</b> and <b>in-mapper combining</b>.

There is two Hadoop Map-Reduce jobs in the same file /src/main/java/Histogram.java. 
Each one of this Map-Reduce jobs will read the same input file but will produce output to a different output directory: in the Java main program, 
both Map-Reduce jobs will read args[0] as the input file with the pixels (which is pixels-small.txt or pixels-large.txt).

The first Map-Reduce job will write on the output directory args[1] (which is output) while the second Map-Reduce job will write on the output directory args[1]+"2" (which is output2).

To compile and run the project navigate to the given directory and do the following steps: <br />
mvn install <br />
rm -rf output <br />
~/hadoop-2.6.5/bin/hadoop jar target/*.jar Histogram pixels-small.txt output output2 <br />

