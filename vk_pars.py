#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
#import vk
import logging
#import requests
#import vk_api
#import bs4
import os
import aiovk
import aiohttp
import asyncio
import multiprocessing
import time


from optparse import OptionParser


class CreateTaskQueue(multiprocessing.Process):
    def __init__(self, queue, num_comm, start_ids=1, num_ids_chunk=500, req_per_sec=20):
        multiprocessing.Process.__init__(self)
        self.num_ids_chunk = num_ids_chunk
        self.start_ids = start_ids
        print(self.start_ids)
        self.num_comm = num_comm
        self.req_per_sec = req_per_sec
        self.queue = queue

    def create_ids_str(self, start, n_id_ch,  r_p_sec):
        return [','.join(str(start + n_id_ch * item_p + item_id) for item_id in range(n_id_ch)) for item_p in range(r_p_sec)]

    def run(self):
        print(self.name)
             
        start_id = self.start_ids
        n = 1
        while True:
            if self.queue.qsize() < self.req_per_sec:
                print()
                request_package = self.create_ids_str(self.start_ids, self.num_ids_chunk, self.req_per_sec)
                print(request_package, len(request_package))
                for id_list in request_package:
                    self.queue.put(id_list)
                self.start_ids += self.num_ids_chunk * self.req_per_sec
     
            if self.start_ids > start_id + self.num_ids_chunk * (self.num_comm): 
                print('-----------', n)
                break
                
            time.sleep(1)
        
            
class TaskVkPars(multiprocessing.Process):
    def __init__(self, queue, fields):
        multiprocessing.Process.__init__(self)
        self.queue = queue
        #self.session = session
        self.fields = fields

    async def reqw(self, strquer):
        
        print(strquer, type(strquer))
       
        data = await self.api.groups.getById(group_ids=strquer, fields=self.fields)
        print('\n', len(data))
        #print(data)

    async def main(self):
        async with aiovk.TokenSession(access_token=token_read()) as session:
            self.api = aiovk.API(session)
            while True:
                strquer = self.queue.get()
                if strquer is None:
                    break
                try:
                    await asyncio.wait_for(self.reqw(strquer), timeout=10)
                except asyncio.TimeoutError:
                    log.info('Error timeout request_period')
                await asyncio.sleep(1)
                print(self.queue.qsize())
               
        
    def run(self):
        self.proc_name = self.name
        print(self.proc_name)
        asyncio.run(self.main())
        log.info('Will be create %s processes' % self.proc_name)
           

def token_read():
    with io.open('token.txt') as file:
        token = file.readline()
    return token

def login_read():
    with io.open('login.txt') as file:
        token1 = file.readline()
        token2 = file.readline()
       
    return token1, token2


async def main():
    cpu_num = multiprocessing.cpu_count()
    print(cpu_num)
    queue = multiprocessing.Queue()
    
    taskq = CreateTaskQueue(queue, 2, num_ids_chunk=5)
    taskq.start()
    #async with aiovk.TokenSession(access_token=token_read()) as session: #
    filds ='site,status,type,city,country,contacts,description,wiki_page,start_date,activity,status,age_limits,main_section,members_count,place'
    tasks = [TaskVkPars(queue, filds) for _ in range(2)]


    for t in tasks:
        t.start()

    taskq.join()
    for _ in tasks:
        queue.put(None)
    print('Close')
    for t in tasks:
        t.join()        
    print('close2')  
        
  
if __name__ == "__main__":
    op = OptionParser("Script parsing VK")
    op.add_option("-l", "--log", action="store", default=None, help="logging space")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', 
                        datefmt='%Y.%m.%d %H:%M:%S')
    log = logging.getLogger()
    try:    
        log.info("Start")
        asyncio.run(main())
    except KeyboardInterrupt:
        pass