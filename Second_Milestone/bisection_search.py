# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 14:10:51 2015

@author: honghuang
"""

import sys
import subprocess

    
def main():
    print 'Hello there'+' hahaha'
    bisection(0.0,1.0)
def bisection(h,t):
    head=h
    tail=t
    mid=(head+tail)/2
    headvalue=blackbox(head)
    print headvalue
    tailvalue=blackbox(tail)
    print tailvalue
    while abs(tail-head)>0.00001:
        mid=(head+tail)/2
        midvalue=blackbox(mid)
        if midvalue==tailvalue:
            tail=mid
            
        elif midvalue==headvalue:
            head=mid
        else:
            bisection(head,mid)
            bisection(mid,tail)
            return
    
    print "The transition point ",mid   

def blackbox(argument):
    subprocess.call(["/home/honghuang/Documents/CPM/Louvain_20110526/bin/community","-o","/home/honghuang/Documents/CPM/Louvain_20110526/sample_networks/result.txt","-pp",str(argument),"/home/honghuang/Documents/CPM/Louvain_20110526/sample_networks/example.bin","/home/honghuang/Documents/CPM/Louvain_20110526/sample_networks/example.conf"])

    value=nc()
    return value
    
def nc():
    dict={}
    print 'Hello there'+' hahaha'
    f = open('/home/honghuang/Documents/CPM/Louvain_20110526/sample_networks/result.txt', 'r')
    for line in f:
        c=line.split('\t')[-1]
        if c in dict:
            count=dict[c]
            count=count+1
            dict[c]=count
        else:
            dict[c]=1
        
    
    sum=0
    for key, value in dict.items():
        sum+=value*value
    return sum
if __name__ == '__main__':
    main()    
