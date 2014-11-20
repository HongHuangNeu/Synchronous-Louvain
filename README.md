Synchronous-Louvain
===================
Created by Hong Huang
In the code, I built a toy example to verify the correctness of one iteration of the program. The toy example is this:
![The toy example](simulation.png)

The numbers inside the circle are vertice id and all the other numbers are edge weights. I have added self loop on purpose to verify the correctness of the program. The weight of selft loop is twice the weight of the community that the current node represents. For instance, you can see that node 3 has a self loop of weight 1.0, so in the previous level the community that node 3 represents now has a total internal weight of 0.5, 0.5x2=1.

Initisally, every node is in his own community. The total weight of edges in this graph is 12, so m is 12, 2m is 24.

For node 3, the sum of the weights of the links incident to nodes in the community(sigma tot for community 3) is 4.0. 
For node 5, the sum of the weights of the links incident to nodes in the community(sigma tot for community 5) is 10.0. 
For node 7, the sum of the weights of the links incident to nodes in the community(sigma tot for community 7) is 6.0.  
For node 2, the sum of the weights of the links incident to nodes in the community(sigma tot for community 2) is 4.0.

The sum of the weights of the links incident to node 3 is 1.0+1.0+2.0=4.0, so k_i=4.0 for node 3. 
The sum of the weights of the links incident to node 7 is 1.0+1.0+4.0=6.0, so k_i=6.0 for node 7.
The sum of the weights of the links incident to node 2 is 1.0+3.0=4.0, so k_i=4.0 for node 2.
The sum of the weights of the links incident to node 5 is 1.0+3.0+4.0+2.0=10.0, so k_i=10.0 for node 5.

For node 3:If he stays in the current community, the gain is 0 because you are doing nothing(removing a node from the community of its own and then add it to the community of its own). If he joins community 7, the weight from community 7 to node 3 is 1.0, so k_i_in is 
