#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import os
import time
import json
import aiovk
import hashlib
import logging
import aiohttp
import asyncio
import multiprocessing
from optparse import OptionParser
from store import RedisStore
from classific import group_classification
from bd_cwr import connect_bd, create_tab, insert_in_table


NUM_PROC_TASK_VK_PARS = 2
FIELDS_OUT = ['id', 'name', 'city','country', 'description',  'age_limits', 'activity', 'members_count']


op = OptionParser("Script parsing VK")
op.add_option("-l", "--log", default=None, help="Logging space, default stdout")
op.add_option("-c", "--config", default="config.json", help="Script config in json format")
op.add_option("-b", "--bdata", default="vkmybd", help="Database name of processed information")
op.add_option("-s", "--startid", default=24199209, help="Start id for parsing communities vk")
op.add_option("-n", "--numpackco", default=20, help="Nunber of community processing packages")
op.add_option("-p", "--packreq", default=400, help="Number of community ids in the request (max = 500, default=400)")
op.add_option("-a", "--access", default="psswd.json", help="Storage file for accessing resources")
(opts, args) = op.parse_args()


def write_file(data):
    with open('trainnews.txt', 'wt', encoding='utf-8') as file:
        file.write(data)
    

class CreateTaskQueue(multiprocessing.Process):
    def __init__(self, queue, num_comm, start_ids=1, num_ids_chunk=100, req_per_sec=20):
        multiprocessing.Process.__init__(self)
        self.num_ids_chunk = num_ids_chunk
        self.start_ids = start_ids
        self.num_comm = num_comm
        self.req_per_sec = req_per_sec
        self.queue = queue

    def create_ids_str(self, start, n_id_ch,  r_p_sec):
        return [','.join(
            str(start + n_id_ch * item_p + item_id
            ) 
            for item_id in range(n_id_ch)) 
            for item_p in range(r_p_sec)
            ]

    def run(self):
        start_id = self.start_ids
        n = 1
        while True:
            if self.queue.qsize() < self.req_per_sec:
                request_package = self.create_ids_str(self.start_ids, self.num_ids_chunk, self.req_per_sec)
                for id_list in request_package:
                    self.queue.put(id_list)
                self.start_ids += self.num_ids_chunk * self.req_per_sec
            if self.start_ids > start_id + self.num_ids_chunk * (self.num_comm): 
                break
            time.sleep(1)
       
            
class TaskVkPars(multiprocessing.Process):
    def __init__(self, queue, token, fields, store, queue_store, num_cor_tasks = 4, fields_out=FIELDS_OUT):
        multiprocessing.Process.__init__(self)
        self.queue = queue
        self.fields = fields
        self.token = token
        self.stop = False
        self.store = store
        self.queue_store = queue_store
        self.num_cor_task = num_cor_tasks
        self.fields_out = fields_out

    def extract_info(self, data):
        list_comm = []
        for item in data:
            dict_info_comm = {}
            for field in self.fields_out:
                if field in item:
                    dict_info_comm[field] = item[field]
                else:
                    dict_info_comm[field] = ''
            list_comm.append(dict_info_comm)
        return list_comm

    async def reqw(self):
        try:
            strquer = self.queue.get(timeout=1)
            if strquer is None:
                self.stop = True
                return
            data = await self.api.groups.getById(group_ids=strquer, fields=self.fields)
            list_comm = self.extract_info(data)
            str_list_comm = json.dumps(list_comm).encode('utf-8')
            key = 'rid' + hashlib.md5(str_list_comm).hexdigest()
            if self.store.set(key, str_list_comm, 360):
                self.queue_store.put(key)
        except Exception as e:
            log.info(e)
   
    async def start_task(self):
        tasks_cor = []
        for _ in range(self.num_cor_task):
            tasks_cor.append(acincio.create_task(reqw()))
        await asyncio.gather(*tasks_cor)

    async def main(self):
        async with aiovk.TokenSession(access_token=self.token) as session:
            self.api = aiovk.API(session)
            while True:
                if self.stop:
                    break
                try:
                    await asyncio.wait_for(self.reqw(), timeout=10)
                except asyncio.TimeoutError:
                    log.info('Error timeout request_period')
                await asyncio.sleep(1)
                
    def run(self):
        self.proc_name = self.name
        log.debug(self.proc_name)
        asyncio.run(self.main())
        log.info('Will be create %s processes' % self.proc_name)


def classif_comm(data_train, queue_store, store, bd):
    print(data_train)
    while True:
        try:
            keys = queue_store.get(timeout=1)
        except:
            continue
        if keys is None:
            break
        data = json.loads(store.get(keys))
        
        '''classif = []
        for elem in data_train.keys():
            result = group_classification(data, data_train, elem, ['name', 'description'])
            c = []
            for index in range(len(result)):
                c.append({elem:result[index]})
            
            classif.append()
        for index in '''


def main(opts):
    settings, secur = configs_load(opts)
    print(settings)
    store = RedisStore()
    con, curs = connect_bd(db=opts.bdata, passw=secur['security']['pswdbd'])
    try:
        create_tab(con, curs, "base_comm", {"id":"INT PRIMARY KEY", 'name':"VARCHAR(80)",'classif':"VARCHAR(80)"})
        create_tab(con, curs, "loc_comm", {"id":"INT PRIMARY KEY", 'country':"VARCHAR(80)",'city':"VARCHAR(80)"})
        create_tab(con, curs, "aud_comm", {"id":"INT PRIMARY KEY", 'nmembers':"INT",'age':"VARCHAR(4)"})
    except Exception as e:
        log.error(e.args[0])
    queue = multiprocessing.Queue()
    queue_store = multiprocessing.Queue()
    taskq = CreateTaskQueue(queue, opts.numpackco, start_ids=opts.startid , num_ids_chunk=opts.packreq)
    taskq.start()
    filds = settings['paramreq']["filds"]
    tasks = [TaskVkPars(
        queue, 
        secur['security']['token_vk'], 
        filds, 
        store, 
        queue_store
        ) for _ in range(NUM_PROC_TASK_VK_PARS)]
    for t in tasks:
        t.start()
    taskclsf = multiprocessing.Process(
        target=classif_comm, 
        args=(settings['train'], 
        queue_store, 
        store, 
        [con, curs]))
    taskclsf.start()

    taskq.join()
    for _ in tasks:
        queue.put(None)
    print('Close')
    for t in tasks:
        t.join() 
    queue_store.put(None)
           
    print('close2')
    taskclsf.join()
    print('close3')
        
def configs_load(opts):
    with open(opts.config, 'r') as file:
        settings = json.load(file)
    with open(opts.access, 'r') as file:
        securyty = json.load(file)
    return settings, securyty


if __name__ == "__main__":
    logging.basicConfig(filename=opts.log, level=logging.INFO, 
        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S'
    )
    log = logging.getLogger()
    try:    
        log.info("Start")
        main(opts)
        #asyncio.run(main(opts))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.error(e)