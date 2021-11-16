import requests
import json
import mysql.connector
import os
import datetime
import asyncio
from aiohttp import ClientSession

mysqldb = mysql.connector.connect(host="192.168.1.110", user="root",
                                      password="tongyuan2512", database="spider", charset="utf8")
mycursor = mysqldb.cursor()

# 隧道域名:端口号
tunnel = "tps254.kdlapi.com:15818"

# 用户名密码方式
username = "123"
password = "123"
proxies = {
    "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
    "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
}

async def main_process(file_list):
    for file in file_list:
        try:
            data = {"Id": file[-13:-4]}

            async with ClientSession() as session:
                async with session.post("https://d.wanfangdata.com.cn/Detail/Thesis/", data=json.dumps(data), proxy="http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}) as response:
                    response = await response.read()
                    response = response.decode()
                    response_dict = json.loads(response)
                    keywords, title, author, school, major, publish_year, degree, tutor, abstract = " ", " ", " ", " ", " ", " ", " ", " ", " "
                    if "detail" in response_dict and response_dict["detail"]:
                        content = response_dict["detail"][0]["thesis"]
                        keywords = content["MachinedKeywords"]
                        title = content["Title"]
                        author = content["Creator"]
                        school = content["OrganizationNorm"]
                        major = content["Major"]
                        publish_year = content["PublishYear"]
                        degree = content["Degree"]
                        tutor = content["Tutor"]
                        abstract = content["Abstract"]

                        key = ""
                        for keyword in keywords:
                            key += keyword + ";"
                        abstract = abstract[0] if abstract else ""
                        tutor = tutor[0] if tutor else ""
                        school = school[0] if school else ""
                        author = author[0] if author else ""
                        title = title[0] if title else ""

                        sql = "INSERT INTO master_phd_info (filename, keywords, title, author, school, major, publish_year, degree, tutor, abstract, file_path) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        val = (file[:-4], key, title, author, school, major, str(publish_year), degree, tutor,
                                          abstract, "学位全文" + "\\" + file)
                        mycursor.execute(sql, val)
                        mysqldb.commit()
                    else:
                        with open("missed_data.txt", "a") as outfile:
                            outfile.write(file + "\n")
                        print("477:" + file)
        except Exception as e:
            with open("missed_data.txt", "a") as outfile:
                outfile.write(file + "\n")
            print(e)

async def main(list_1, list_2, list_3, list_4, list_5):

    tasks = []


    tasks.append(main_process(list_1))
    tasks.append(main_process(list_2))
    tasks.append(main_process(list_3))
    tasks.append(main_process(list_4))
    tasks.append(main_process(list_5))

    await asyncio.gather(*tasks)




if __name__ == '__main__':
    start = datetime.datetime.now()


    file_list = []
    for root, dirs, files in os.walk(r"学位全文"):
        for dir in dirs:
            for r, d, f in os.walk(r"学位全文" + "\\" + dir):
                for file in f:
                    file_list.append(dir + "\\" + file)
    # main_process(file_list)
    list_1 = file_list[:300]
    list_2 = file_list[300:600]
    list_3 = file_list[600:900]
    list_4 = file_list[900:1200]
    list_5 = file_list[1200:]

    asyncio.run(main(list_1, list_2, list_3, list_4, list_5))

    end = datetime.datetime.now()
    print(end-start)
    # main()
