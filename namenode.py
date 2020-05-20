#coding:utf-8
import sys
import hashlib
import os

siz=1024*1024*2

def read_bigFile(st):
    ulist=[]
    f = open(st,'r')
    cont = f.read(siz)
    while len(cont) >0 :
        ulist.append(cont)
        cont = f.read(siz)
    f.close()
    return ulist


class nameserver:
    def f(self):
        return "f_namenode"

class dataserver:
    def f(self):
        return "f_dataserver"
    def run(self):
        return ""
    def readfile(self):
        return ""
    def writefile(self):
        return ""


class minidfs:
    def f(self):
        return ""

class client:
    idx=0
    nn=0
    ss=""
    def write(self,ss):
        path=ss
        ulist=read_bigFile(path)
        all_id=[]
        l=len(ulist)
        tmp_num=1
        for ul in ulist:
            num_id=[]
            tmp_path=path+'_'+str(tmp_num)
            tmp_num+=1
            self.distribute_write(num_id,tmp_path,ul)
            all_id.append(num_id)
        siz=os.path.getsize(path)
        self.update_info(path,siz,md5sum(path).hexdigest(),l,all_id)

    def update_info(self,name,siz,md5,l,all_id):
        path='data/name1/info'
        f=open(path,'a')
        f.write('* '+name+' '+str(siz)+' '+md5+' '+str(l)+'\n')
        for i in range(l):
            f.write('#'+str(i)+": ")
            st=""
            for j in all_id[i]:
                st+=str(j)+' '
            st+='\n'
            f.write(st)

    def distribute_write(self,num_id,name,ul):
        for num in range(3):
            path1="data/data_"+str(self.nn)+"/"+name
            f=open(path1,'w')
            f.write(ul)
            f.close()
            num_id.append(self.nn)
            self.nn+=1
            self.nn%=4
        return num_id

    def read_whole(self,name):
        path='data/name1/info'
        f=open(path,'r')
        idx=[]
        flag=False
        lenth=0
        for line in f.readlines():
            line=line.strip().split(' ')
            if line[0]=='*':
                flag=False
                if line[1]==name:
                    lenth=int(line[4])
                    flag=True
                    continue
                else:
                    continue
            else:
                if flag:
                    idx.append(int(line[1]))
        cont=""
        for i in range(lenth):
            tidx=idx[i]
            t=i+1
            nam='data/data_'+str(tidx)+'/'+name+'_'+str(t)
            f=open(nam,'r')
            cont+=f.read()
            f.close()
        print(cont)

    def process(self,ss):
        ss=ss.strip().split()
        if ss[0]=='read_whole':
            self.read_whole(ss[1])
        if ss[0]=='read':
            #print(ss)
            self.read(ss[1],int(ss[2]),int(ss[3]))
        if ss[0]=='write':
            self.write(ss[1])
        if ss[0]=='ls':
            self.showfilestorage()

    def __init__(self,st):
        self.ss=st
        self.process(st)
    def deletedataserver(self):
        return ""

    def showfilestorage(self):
        f=open('data/name1/info','r')
        for line in f.readlines():
            print(line,end='')

    def read(self,name,offset,lent):
        path='data/name1/info'
        f=open(path,'r')
        idx=[]
        flag=False
        lenth=0
        for line in f.readlines():
            line=line.strip().split(' ')
            if line[0]=='*':
                flag=False
                if line[1]==name:
                    lenth=int(line[4])
                    flag=True
                    continue
                else:
                    continue
            else:
                if flag:
                    idx.append(int(line[1]))

        id1=int(offset/siz)
        id2=offset%siz
        #print(idx)
        cont=""

        tidx=idx[id1]
        t=id1+1
        nam='data/data_'+str(tidx)+'/'+name+'_'+str(t)
        f=open(nam,'r')

        ####重点
        f.seek(id2)
        cont+=f.read(lent)
        f.close()
        print(cont)

    def help(self):
        #print(self.ss)
        return ""
    def exit(self):
        return ""
    def f(self):
        return "user_input"


class fileinfo:
    def f(self):
        return "fileinfo"


def md5sum(fname):
    file_object = open(fname,'rb')
    file_content = file_object.read()
    file_object.close()
    file_md5 = hashlib.md5(file_content)
    return file_md5

class task:
    def f(self):
        return "task"

class usercommand:
    def  f(self):
        return "usercommand"


if __name__=='__main__':
    in_ = input("please write your demand:")
    client1=client(in_)
    #client1.write(input())
    #client1.read('')
