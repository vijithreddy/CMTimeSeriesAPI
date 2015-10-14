# CMTimeSeriesAPI

To run this Java program one has to enter the following command line arguments:

Hostname Cluster-username Cluster-password tsQuery fromDate(yyyy-MM-dd) toDate(yyyy-MM-dd)

An example of the arguments can be:

ec2-aws.com admin admin "select cpu_percent_across_hosts where category = CLUSTER" 2015-10-10 2015-10-14

The program retrieves peak usage metrics for each day between your fromDate and toDate. From the above example dates the program retrieves metrics for 2015-10-10 to 2015-10-11, 2015-10-11 to 2015-10-12, 2015-10-12 to 2015-10-13, and 2015-10-13 to 2015-10-14

Future work: The metrics are now printed onto the screen, but one can write code to export the metrics to a CSV or a JSON file.
