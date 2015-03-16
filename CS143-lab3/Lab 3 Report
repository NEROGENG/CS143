# CS143
Lab 3 Report

Jonathan Ong  104135858
Quanjie Geng	204160668

Our design for join ordering is based off of the Selinger Optimizer (pseudocode provided in the spec). We used PlanCache to store our computed optjoin and enumerateSubsets to achieve choose(|j|, i) where i is from 1 to |j|. 
We made a little change to the API in estimateJoinCardinality. While the spec says that we should fill in the estimateJoinCardinality function, in the skeleton code, this function actually makes a call to estimateTableJoinCardinality. We feel like this extra function call is unnecessary, so we sticked to the spec and put our code in estimateJoinCardinality and comment out the call to estimateTableJoinCardinality. 
Our design implements all the required functions of simpledb. 
For the IntHistogram, and array was chosen for the histogram.  Instead of storing the values, we simply needed to convert the valid value into the proper bucket and increment the count.  The width was assumed to be the same across all the buckets.  The width was at least 1 and could be greater if the range was larger than the bucket number provided during construction.  Selectivity was calculated by dividing the number of proper values and dividing by the total number.
For Tablestats, minimum and maximum values are first calculated for each tuple so histograms can be calculated.  Once the histograms are initialized with the proper size and values, values are entered in one at time to allow the selectivity functions to properly work.  Estimating selectivity relies on the IntHistogram and StringHistogram implementation.
Difficulties were had when selectivity was tested.  There were problems with checking valid indexes and problems with division.
NOTE: BECAUSE THIS PROJECT WAS TURNED IN 3 DAYS LATE (SUNDAY), 3 FLEX DAYS WERE CONSUMED.  BOTH JONATHAN AND QUANJIE HAD 3 SPARE DAYS, AND THEY HAVE NONE LEFT*

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lab 2  Report 	

Design based off of parameters and algorithms mentioned in textbook.  Most of aggregate design coded to fit unit tests and system tests.
Code was briefly noted during the first week, and coding began during the second week after the midterm.  
Not many difficulties until the aggregates.  They required familiarity with the rest of the project's constants. Also had to sort out the different data structures and cases for grouping and not grouping.

This Lab was completed and submitted on time, February 11, 2015.  Therefore, no flex days were consumed and the team members have 3 each.



~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lab 1 report
No changes to APIs.
Design based off of paramters and return types of functions implemented.
Ample time was spent familiarizing ourselves with the lab and the flow of the program.  Difficulties were had when testing our implementation on the VMs. Each night, a file or two were implemented.  

We ran into many problems from the beginning: iterators not working, tuples not being converted to strings and many other small things.  When we ran our system tests and the classes we wrote started interacting with each other, there were two main bugs.  When we read from file, we originally believed that the offset parameter offset the pointer to the file, and we ran into problems with what was contained in each page file generated.  THe other bug was when we started comparing larger tests; our hash function didn't work as intended and we mistakingly hashed pages to collide, causing tuples to appear unexpectedly.

Because this lab was turned in 1 day late, 1 flex day was used for both Jonathan and Quanjie.  Of the 4 total flex days, both have 3 flex days left.
