#!/bin/bash

# Установить rwxrwxrwx для директорий
sudo find . -type d -exec chmod 0777 {} \;
# Установить rwxrwxrwx для исполняемых файлов (100755)
sudo find . -type f -perm -u+x -exec chmod 0777 {} \;
# Установить rw-rw-rw- для неисполняемых файлов (100644)
sudo find . -type f ! -perm -u+x -exec chmod 0666 {} \;
# Установить setgid для наследования группы
sudo find . -type d -exec chmod g+s {} \;
# Установить дефолтные ACL для новых файлов/папок
sudo setfacl -R -m d:u::rwx,d:g::rwx,d:o::rwx,d:m::rw .
