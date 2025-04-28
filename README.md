# pkafka_final
Финальный проект


block
block-table
products_in
products_out
analytics



http://localhost:9870/ # hadoop
http://localhost:8080/ # spark

добавить в /etc/hosts
192.168.0.101 b1.kafka.local
192.168.0.102 b2.kafka.local
192.168.0.103 b3.kafka.local

192.168.0.111 br1.kafka.local
192.168.0.112 br2.kafka.local
192.168.0.113 br3.kafka.local


kafka connect находится по адресу http://localhost:8083/
зарегестрированные коннекторы можно посмотреть выполнив GET http://localhost:8083/connectors

kafka web ui находится по адресу http://localhost:8088/ui/

b1.kafka.local 
    19092
    9092
    9192
b2.kafka.local
b3.kafka.local

br1.kafka.local
br2.kafka.local
br3.kafka.local

? sr.kafka.

graph.local


###
1. Мониторинг 3ч
2. ClientAPI 2ч
3. Analytics 4ч
    1. Коннектор HDFS
    2. Запись из продьюсера в hdfs
    3. Придумать какую-нибудь аналитику
    4. Отправка консьюмеорм в топик с рекомендациям (топик с настройками compact)
4. Вынести настройки в конфиги 2ч
5. Сделать запуск сервисов из одного места 1ч
6. View для просмотра рекомендаций 1ч
7. ACL 1ч
8. Оформление 2ч
9. Проверка 3ч

