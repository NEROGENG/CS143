# CS143
Lab 1 Report 
Jonathan Ong  	104135858
Quanjie Geng	204160668	

No changes to APIs.
Design based off of paramters and return types of functions implemented.
Ample time was spent familiarizing ourselves with the lab and the flow of the program.  Difficulties were had when testing our implementation on the VMs. Each night, a file or two were implemented.  

We ran into many problems from the beginning: iterators not working, tuples not being converted to strings and many other small things.  When we ran our system tests and the classes we wrote started interacting with each other, there were two main bugs.  When we read from file, we originally believed that the offset parameter offset the pointer to the file, and we ran into problems with what was contained in each page file generated.  THe other bug was when we started comparing larger tests; our hash function didn't work as intended and we mistakingly hashed pages to collide, causing tuples to appear unexpectedly.

Because this lab was turned in 1 day late, 1 flex day was used for both Jonathan and Quanjie.  Of the 4 total flex days, both have 3 flex days left.