#coding:utf-8
siz=1024*1024*2
def read_bigFile():
    f = open("text1",'r')
    cont = f.read(1)
    while len(cont) >0 :
        print(cont)
        cont = f.read(1)
    f.close()

#read_bigFile()