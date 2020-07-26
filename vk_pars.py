#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import vk
import logging
import requests
import vk_api
import bs4
import os
import aiovk
import aiohttp
import asyncio
import multiprocessing


from optparse import OptionParser


class CreateTaskQueue(multiprocessing.Process):
    def __init__(self, queue, num_comm, start_ids=1, num_ids_chunk=500, req_per_sec=15):
        super().__init__(self)
        self.num_ids_chunk = num_ids_chunk
        self.start_ids = start_ids
        self.num_comm = num_comm
        self.req_per_sec = req_per_sec
        self.queue = queue

    def create_ids_str(self, list_ids):
        return ','.join(str(id) for id in list_ids)

    def run(self):
        request_package = []
        for item_id in range(self.num_comm//self.num_ids_chunk):
            request_package.append(
                [self.start_ids + item + item_id*self.num_ids_chunk for item in range(self.num_ids_chunk)]
                )
        else:
            if self.num_comm % self.num_ids_chunk:
                request_package.append(
                    [self.start_ids + self.num_ids_chunk * (item_id + 1) + 1 for item in range(self.num_comm % self.num_ids_chunk)]
                    )
        for ind_list, item_id in enumerate(request_package):
                if self.queue.qsize() < self.req_per_sec:
                    self.queue.put(self.create_ids_str(request_package))
                if not ind_list % self.req_per_sec and ind_list:
                    time.sleep(1)


class TaskVkPars(multiprocessing.Process):
    
    def __init__(self, queue):
        multiprocessing.Process.__init__(self)
        self.queue = queue

    async def reqw(self):
        print(self.name)

    async def main(self):
        while True:
            try:
                await asyncio.wait_for(self.reqw(), timeout=10)
            except asyncio.TimeoutError:
                log.info('Error timeout request_period')
            await asyncio.sleep(2)
            break
        
    def run(self):
        self.proc_name = self.name
        print(self.proc_name)
        
        task = asyncio.run(self.main())
        log.info('Will be create %s processes' % self.proc_name)
           
            
            



def get_members(vk_api, groupid):  # Функция формирования базы участников сообщества в виде списка
    first = vk_api.groups.getMembers(group_id=groupid, v=5.92)  # Первое выполнение метода
    data = first["items"]  # Присваиваем переменной первую тысячу id'шников
    count = first["count"] // 1000  # Присваиваем переменной количество тысяч участников
    # С каждым проходом цикла смещение offset увеличивается на тысячу
    # и еще тысяча id'шников добавляется к нашему списку.
    for i in range(1, count+1):  
        data = data + vk_api.groups.getMembers(group_id=groupid, v=5.92, offset=i*1000)["items"]
    return data


def vk_group_user(vk_api, name_user):
    #s = vk_api.groups.getById(group_id=name_user, v=5.118)
    #s = vk_api.users.get(user_ids= [455547568, 606046649], need_html=1)
    m = vk_api.get('https://vk.com/id606046649')
    #s = vk_api.groups.get(cuser_id=9692363, v=5.118)
    #n = vk_api.groups.getMembers(group_id=name_user, v=5.118)
    return m #s #, n


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
    
    tasks = [Task(queue) for _ in range(cpu_num)]
    
    for t in tasks:
        t.start()

    for t in tasks:
        t.join()

    async with aiovk.TokenSession(access_token=token_read()) as session: #
        filds ='site,status,type,city,country,contacts,description,wiki_page,start_date,activity,status,age_limits,main_section,members_count,place'
        
        api = aiovk.API(session)
        req = ''
        for i in range(45678,45678+100):
            req += str(i)+','
        req += '191527376'
        print(req)
        data = await api.groups.getById(group_ids=req, fields=filds)
        #data2 = await api.database.getCities(country_id=1, offset=100000, count=1000 ,need_all=1)
        data1 = await api.groups.getMembers(group_id=191527376)
        
        for i in data:
            if 'description' in i:
                print(i['name'], i['activity'],'\n', type(i))  
        #print(data)

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