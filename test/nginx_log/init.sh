hadoop fs -put ./nginx_200.log "/flume/nginx_logs/$(date +'%Y%m%d')"
hadoop fs -put ./nginx_404.log "/flume/nginx_logs/$(date +'%Y%m%d')"