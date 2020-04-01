#!/bin/bash
step=1 #间隔的秒数，不能大于60

user_agent_list=("Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0")
user_agent_list[1]="Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

referer_list=("https://www.baidu.com" "https://www.qq.com" "https://www.sina.com" "https://weibo.com/")

while [ 1 ]
do
    random=$((RANDOM))
    num=$(((RANDOM%7)+1))
    agent=$(((RANDOM%2)))
    referer=$(((RANDOM%4)))
    url="http://172.16.1.126:8881/"$num".html?r="$random;
    url="http://172.16.1.126:8881/888.html";
    echo " `date +%Y-%m-%d\ %H:%M:%S` get $url"

    #curl http://192.168.75.137/1.html #调用链接
    curl -s -A "${user_agent_list[$agent]}" -e "${referer_list[$referer]}" $url > /dev/null

    sleep $step
done