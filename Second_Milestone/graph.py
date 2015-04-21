# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 22:31:14 2015

@author: honghuang
"""
from heapq import *
import matplotlib.pyplot as plt
import pandas as pd
import louvain
from igraph import * 
from datetime import datetime
if __name__ == '__main__':
    #g=Graph.read("/home/honghuang/exampleGraph", format="ncol", directed=False, names=True)
    file1="/home/honghuang/Documents/benchmark/100000/network.dat"
    file2="/home/honghuang/exampleGraph"
    file3="/home/honghuang/Synchronous-Louvain/Second_Milestone/samples/ForestFire/50000/ff-50000-2"
    file4="/home/honghuang/Documents/benchmark/100000/sample3.dat"
    file5="/home/honghuang/Documents/benchmark/1000_1/network.dat"
    file6="/home/honghuang/Documents/benchmark/10000_1/network.dat"
    file7="/home/honghuang/Synchronous-Louvain/benchmark/binary_network_5000/5000_0.35/network.dat"
    file8="/home/honghuang/Documents/benchmark/binary_networks/network.dat"
    file9="/home/honghuang/trySample1"
    g=Graph.Read_Ncol(file9, names=True,  weights="if_present", directed=False)
    g.simplify()
    for edge in g.es():
        print edge.tuple[0], edge.tuple[1]#, edge["weight"]
    
    #part = louvain.find_partition(g, method='CPM',resolution_parameter=0.5);
    #part.significance = louvain.quality(g, part, method='Significance',resolution_parameter=0.5);
    #print part.significance
    
    res_parts = louvain.bisect(g, method='CPM', resolution_range=[0,1]);
    res_df = pd.DataFrame({
         'resolution': res_parts.keys(),
         'bisect_value': [bisect.bisect_value for bisect in res_parts.values()]});
    plt.step(res_df['resolution'], res_df['bisect_value']);
    plt.xscale('log');
    
    print str(datetime.now())
    
    #resolution profile file    
    f = open('/home/honghuang/profile', 'w')
    
    stack=[]    
    sortList=[]
    longestLength=0.0
    tup=(0.0,0.0)
    for k in res_parts.keys():
        
        if len(stack)!=0:
            head=stack[len(stack)-1]
            length=log10(k)-log10(head)
            
            if(head!=0.0 and k!=1.0):
                heappush(sortList,(length,(head,k)))
            
            
            if(length>longestLength and head!=0.0 and k!=1.0):
                longestLength=length
                tup=(head,k)
        stack.append(k)
        print k
        part = louvain.find_partition(g, method='CPM',resolution_parameter=k);
        part.significance = louvain.quality(g, part, method='Significance',resolution_parameter=k);
        print "the significance with resolution",k,"is",part.significance
        f.write(str(k)+" "+str(part.significance)+"\n")
   
    result=[heappop(sortList) for i in range(len(sortList))]  
    
    for t in result:
        print t
        
        
    print "result",tup
    f.flush()