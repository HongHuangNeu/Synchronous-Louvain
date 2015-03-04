# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 16:50:08 2015

@author: honghuang
"""

def main():
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
    print sum
if __name__ == '__main__':
    main()   