This project was completed by the group: 
Jeffrey Jiang (904255069): jiangxuanzhi@gmail.com
Aaron Chung (804288857): aaronchung@ucla.edu

We did this project by peer programming. We are turning in Project 2C 1 day late. 

2B: 
Since I forgot to change the readme in part 2B, I will include part 2B read me here
in hopes that this will not make a huge difference of where it is, but I didn't want 
to resubmit the project late just to add stuff onto the readme. 

Implememtation Details: 
    We defined four structs to represent the different things in a standard B+-tree. 
    The reason we chose to do this is to provide a more readable method by which we 
    can store information, while also providing the ability to make modular changes through
    the use of functions such as sizeof(). 
    LeafNode: 
        LeafNodeHeader - contains info about its pid, surrounding nodes, and capacity. 
        LeafPair - contains the key, rid pair of a leaf node. 
    NonLeafNode: 
        NonLeafHeader - contains info about its capacity, and the first pid (left most pointer). 
        NodePair - contains the key, and a pid. 
            the pid that it has refers to the pointer that is greater than the key. 
    The reason we decided to have the NonLeafNode pointers in this manner is that we noticed
    the following property: once the left-most pointer is defined, it will never be changed to
    a different pid. The value of the first key may change, but this will only result in 
    changes within the child node, and not any changes to the pointer itself. We hope that 
    this property is true, as it makes everything else a lot more simplistic. (this is only 
    true in our "only insert" scenario as tested). 

    Based on these, we defined a couple of public helper functions to properly initialize these
    values and used them to properly define the functionality of the B+-tree. One very useful
    function that we defined that made this much easier for us was the private member function
    getByteIndexof(n), which allows us to get which Pair we wanted in the buffer in bytes, so 
    that we could access them with pointers. 

    Other than these additions, we mostly implemented the code as the specs wanted us to. 


2C: 
Implementation Details: 
    We simply implemented the three functions that we were supposed to. 
    
    There were a couple things that we found that became complications and solutions. 
    1. Initialization of treeHeight and rootPid. 
        As we realized that we could read and write to any page file, we wanted to somehow
        write the treeHeight and rootPid value to the page file somehow. We took the easy 
        way out by reserving the first page of the page file to ONLY store the information 
        that we need, which we defined in the struct called header. 
    2. Initialization of the B+-tree in terms of the root. 
        Since we were considering how to initialize the B+-tree, we also realized we never
        actually learned how to initialize a B+-tree. Thus, we came to the conclusion that
        we will be using a leaf node as the root until it fills up, which then it will have
        a nonleaf root. 
    3. Maintaining a path of lineage for the locate function to help with insertion. 
        We additionally created a helper fuction for locate to allow us to implement the
        locate method recursively. 

2D: 

We turned in project 2C two days late. However, what we turned in was unfinished. 
Frustrated and unpleased with the results, we continued to work afterward, in hopes
to finish, even if we couldn't get points for the changes that we did. We are now 
submitting a completed version 3 hours after the 2 day deadline. We will let the 
TAs and professors decide what they want to do with this version of the submission, 
but we hope that at least we can get some credit for it. 

Implementation Details: 
    1. When evaluating a select query on a key attribute, we iterate through the list of
		constraints and tighten the "min" and "max" bounds. If at any point we encounter
		a conflicting constraint(i.e. key < 10 and key > 20), then we short circuit the
		process because we know that there will be no matching tuples returned. 
    2. After iterating through all bounds constraining conditions, we evaluate the remaining
		conditions on the subset of keys that fall were not key constraining bounds. If we
		are not evaluating a condition on a key attribute, we simply search through the
		entire table.

Troubles we came across in fixing previous parts: 
    1. Figuring out a segmentation fault caused by not initializing the buffers correctly. 
        This one took the longest time and was the most frustrating. 
    2. Editing some bugs with the B+ tree. 
        We had a good number of our > and < signs backward. 
    3. Our readForward function was definitely not correct in the past. 