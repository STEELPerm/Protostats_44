import time
import random

def GetRandomUserAgent ():
    with open("UserAgent.txt") as file:
        UserAgentAr = [row.strip() for row in file]

    Rand = random.randint(1, len(UserAgentAr))
    return UserAgentAr[Rand]


def chunks(lst, n):
    for i in range(0, len(lst), n):
        print (i)
        yield lst[i:i + n]
        print(lst[i:i + n])

args=['0108200000121000027','0111200002420001170','0112200000821000163','0123200000321000127','0126200000420005750','0126200000421000013','0126200000421000111','0126200000421000239',\
      '0136500001120007692','0139200000121000079','0142200001320024781','0142200001321000735','0142200001321001164','0163200000321000178','0165100007921000033','0167200003421000067']

print(len(args))

chunks = list(chunks(args, 4))
print(chunks)

# print(random.uniform(1,1.5))
# time.sleep(random.uniform(1,1.5))
# print(random.uniform(1,1.5))
#
#
# with open("UserAgent.txt") as file:
#     UserAgentAr = [row.strip() for row in file]
#
# Rand = random.randint(1, len(UserAgentAr))
# UserAgent = UserAgentAr[Rand]
# #print(UserAgent)
#
# headers = {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
#     'Content-type': 'application/json; charset=UTF-8',
#     'User-Agent':  UserAgent
#     # 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
# }
# print(headers)