#D:\Python27
#-*- coding:utf-8 -*-
import jieba
import logging
import sys
import codecs
import traceback
import time

def timestamp_datetime(value):
    format = '%Y-%m-%d %H:%M:%S'
    # value为传入的值为时间戳(整形)，如：1332888820
    value = time.localtime(value)
    ## 经过localtime转换后变成
    ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=0)
    # 最后再经过strftime函数转换为正常日期格式。
    dt = time.strftime(format, value)
    return dt

def datetime_timestamp(dt):
    #dt为字符串
    #中间过程，一般都需要将字符串转化为时间数组
    time.strptime(dt, '%Y-%m-%d %H:%M:%S')
    ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=-1)
    #将"2012-03-28 06:53:40"转化为时间戳
    s = time.mktime(time.strptime(dt, '%Y-%m-%d %H:%M:%S'))
    return int(s)

class dataProcess(object):
    #初始化函数,重写父类函数
    def __init__(self):
        pass


    def ruleone_process(self,process_file):
        corpus_list = []
        try:
            fp = open(process_file, "r")
            fw = open("rule1.txt", "w")

            travel_Cluster = {}
            key = ""
            for line in fp:
                conline = line.strip().split(",")
                key = conline[11]+conline[22]

                if travel_Cluster.get(key,None) == None :

                    travel_Cluster[key]=[]
                    travel_Cluster[key].append(conline)
                else:
                    travel_Cluster[key].append(conline)
            for(Key,Value) in travel_Cluster.iteritems():
                Value_tmp =sorted(Value,key = lambda x:datetime_timestamp(x[6]))
                travel_Cluster[Key] = Value_tmp
            for(Key,Value) in travel_Cluster.iteritems():
                count = 0
                policy_cnt = 1
                item_tmp = None
                claim_cnt = 0
                claim_cmt = 0
                day_set = set()
                day_dis = 1
                for item in travel_Cluster[Key]:
                    if count == 0:
                        if item[17] != "" and item[17] != None and float(item[17]) > 0 :
                                claim_cmt = claim_cmt +float(item[17])
                        print item[0] ,1,claim_cnt,claim_cmt,day_dis
                        fw.write('%s\t%d\t%d\t%f\t%d\n'%(item[0] ,1,claim_cnt,claim_cmt,day_dis))
                    else:
                        if datetime_timestamp(item[6])-datetime_timestamp(item_tmp[6]) <= 2592000:
                            policy_cnt = policy_cnt+1

                            day_set.add(item[6].split(" ")[0])

                            if item[19] != "" and item[19] != None and int(item[19]) > 0 :
                                claim_cnt = claim_cnt +1
                            if item[17] != "" and item[17] != None and float(item[17]) > 0 :
                                claim_cmt = claim_cmt +float(item[17])

                        print item[0] ,policy_cnt,claim_cnt,claim_cmt,len(day_set)
                        fw.write('%s\t%d\t%d\t%f\t%d\n'%(item[0] ,policy_cnt,claim_cnt,claim_cmt,len(day_set)))

                    item_tmp = item
                    count = count +1

            fp.close()
            fw.close()
            return True,travel_Cluster
        except:
            logging.error(traceback.format_exc() )
            return False, "ruleone fail"

    def ruletwo_process(self,process_file):
        corpus_list = []
        try:
            fp = open(process_file, "r")
            fw = open("rule2.txt", "w")
            travel_Cluster = {}
            key = ""
            for line in fp:
                conline = line.strip().split(",")
                key = conline[22]

                if travel_Cluster.get(key,None) == None :

                    travel_Cluster[key]=[]
                    travel_Cluster[key].append(conline)
                else:
                    travel_Cluster[key].append(conline)
            for(Key,Value) in travel_Cluster.iteritems():
                Value_tmp =sorted(Value,key = lambda x:datetime_timestamp(x[6]))
                travel_Cluster[Key] = Value_tmp
            for(Key,Value) in travel_Cluster.iteritems():
                count = 0
                policy_cnt = 1
                item_tmp = None
                claim_cnt = 0
                claim_cmt = 0
                day_set = set()
                day_dis = 1
                for item in travel_Cluster[Key]:
                    if count == 0:
                        if item[17] != "" and item[17] != None and float(item[17]) > 0 :
                                claim_cmt = claim_cmt +float(item[17])
                        print item[0] ,1,claim_cnt,claim_cmt,day_dis
                        fw.write('%s\t%d\t%d\t%f\t%d\n'%(item[0] ,1,claim_cnt,claim_cmt,day_dis))
                    else:
                        if datetime_timestamp(item[6])-datetime_timestamp(item_tmp[6]) <= 2592000:
                            policy_cnt = policy_cnt+1

                            day_set.add(item[6].split(" ")[0])

                            if item[19] != "" and item[19] != None and int(item[19]) > 0 :
                                claim_cnt = claim_cnt +1
                            if item[17] != "" and item[17] != None and float(item[17]) > 0 :
                                claim_cmt = claim_cmt +float(item[17])

                        print item[0] ,policy_cnt,claim_cnt,claim_cmt,len(day_set)
                        fw.write('%s\t%d\t%d\t%f\t%d\n'%(item[0] ,policy_cnt,claim_cnt,claim_cmt,len(day_set)))

                    item_tmp = item
                    count = count +1

            fp.close()
            fw.close()
            return True,travel_Cluster
        except:
            logging.error(traceback.format_exc() )
            return False, "ruleone fail"


    def rulethree_process(self,process_file):
        corpus_list = []
        try:
            fp = open(process_file, "r")
            fw = open("rule3.txt", "w")
            travel_Cluster = {}
            key = ""
            for line in fp:
                conline = line.strip().split(",")
                key = conline[0]
                if conline[1] == "" or conline[1] == None:
                    continue

                if travel_Cluster.get(key,None) == None :

                    travel_Cluster[key]=[]
                    travel_Cluster[key].append(conline)
                else:
                    travel_Cluster[key].append(conline)
            for(Key,Value) in travel_Cluster.iteritems():
                print Value
                Value_tmp =sorted(Value,key = lambda x:datetime_timestamp(x[1]))
                travel_Cluster[Key] = Value_tmp



            for(Key,Value) in travel_Cluster.iteritems():
                count = 0
                stat_nt = 0
                stat_lists = []
                item_tmp = None
                for item in travel_Cluster[Key]:
                    stat_record = []
                    if count == 0:
                        print item[0] ,0
                        fw.write('%s\t%d\n'%(item[0] ,0))
                        # stat_record.append(item[0])
                        #
                        # stat_record.append(0)
                    else:
                        if datetime_timestamp(item[1])-datetime_timestamp(item_tmp[1]) <= 5400 and item[3] != item_tmp[3]:
                            print item[0] ,1
                            fw.write('%s\t%d\n'%(item[0] ,1))

                            # stat_record.append(1)
                        else:
                            fw.write('%s\t%d\n'%(item[0] ,0))
                            # stat_record.append(item[0])
                            #
                            # stat_record.append(0)
                    # stat_lists.append(stat_record)


                    item_tmp = item
                    count = count +1
                # sum = 0
                # for item in stat_lists:
                #     sum = sum +item[1]
                #     print item[0],sum
                #     fw.write('%s\t%d\n'%(item[0] ,sum))


            fp.close()
            fw.close()
            return True,travel_Cluster
        except:
            logging.error(traceback.format_exc() )
            return False, "ruleone fail"

    def rulefour_process(self,process_file):
        corpus_list = []
        try:
            fp = open(process_file, "r")
            fw = open("rule4.txt", "w")
            travel_Cluster = {}
            key = ""
            for line in fp:
                conline = line.strip().split(",")
                key = conline[24]

                if travel_Cluster.get(key,None) == None :

                    travel_Cluster[key]=[]
                    travel_Cluster[key].append(conline)
                else:
                    travel_Cluster[key].append(conline)
            for(Key,Value) in travel_Cluster.iteritems():
                Value_tmp =sorted(Value,key = lambda x:datetime_timestamp(x[6]))
                travel_Cluster[Key] = Value_tmp


            for(Key,Value) in travel_Cluster.iteritems():
                count = 0
                stat_nt = 0
                certid_set = set()
                for item in travel_Cluster[Key]:
                    certid_set.add(item[21])
                    print item[0],len(certid_set)
                    fw.write('%s\t%d\n'%(item[0] ,len(certid_set)))


            fp.close()
            fw.close()
            return True,travel_Cluster
        except:
            logging.error(traceback.format_exc() )
            return False, "ruleone fail"




    def output_file(self,out_file,item):

        try:
            fw = open(out_file, "a")
            fw.write('%s'%(item.encode("utf-8")))
            fw.close()
        except:
            logging.error(traceback.format_exc() )
            return False, "out file fail"


    #释放内存资源
    def __del__(self):
        pass

    def process(self,process_file):
        try:
            travel_list = {}
            #flag, travel_list = self.ruleone_process(process_file)
            #flag, travel_list = self.ruletwo_process(process_file)
            flag, travel_list = self.rulethree_process(process_file)
            #flag, travel_list = self.rulefour_process(process_file)

        except:
            logging.error(traceback.format_exc())
            return False, "process fail"


#类似于主函数
if __name__ == "__main__":
     #获取TextProcess对象
    tp = dataProcess()
    tp.process("t.txt")

    # d = datetime_timestamp('2012-03-28 06:53:40')
    # print d
    # s = timestamp_datetime(1332888820)
    # print s





