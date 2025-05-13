# spark.Dockerfile
ARG SPARK_VERSION=3.2.4

# Этап 1: Сборка Python 3.8 и настройка (твой текущий Dockerfile)
FROM bitnami/spark:${SPARK_VERSION} AS builder

USER root

# 1) Устанавливаем зависимости для сборки и получения Python 3.8
RUN apt-get update && rm -rf /var/lib/apt/lists/* && \
    apt-get update && apt-get install -y --no-install-recommends \
        wget build-essential libssl-dev zlib1g-dev libbz2-dev \
        libreadline-dev libsqlite3-dev curl ca-certificates libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# 2) Собираем Python 3.8 из исходников
RUN cd /tmp && \
    wget https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz && \
    tar xzf Python-3.8.12.tgz && cd Python-3.8.12 && \
    ./configure --enable-optimizations --prefix=/usr/local && \
    make -j$(nproc) && make install && \
    cd / && rm -rf /tmp/Python-3.8.12*

# 3) Настраиваем систему: альтернативы и удаляем Bitnami-Python
RUN update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.8 1 && \
    update-alternatives --set python3 /usr/local/bin/python3.8 && \
    rm -rf /opt/bitnami/python && \
    mkdir -p /opt/bitnami/python/bin && \
    ln -s /usr/local/bin/python3.8 /opt/bitnami/python/bin/python3 && \
    ln -s /usr/local/bin/python3.8 /opt/bitnami/python/bin/python

# 4) Устанавливаем pip для Python 3.8
RUN curl https://bootstrap.pypa.io/pip/3.8/get-pip.py -o get-pip.py && \
    /usr/local/bin/python3.8 get-pip.py && \
    rm get-pip.py

# 5) Жёстко настраиваем переменные окружения Spark, чтобы использовать только Python 3.8
ENV PYSPARK_PYTHON=/usr/local/bin/python3.8 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.8 \
    SPARK_PYTHON=/usr/local/bin/python3.8 \
    SPARK_DRIVER_PYTHON=/usr/local/bin/python3.8 \
    PATH=/usr/local/bin:/usr/bin:/opt/bitnami/python/bin:$PATH

# Этап 2: Финальный образ
FROM bitnami/spark:${SPARK_VERSION}

USER root

# Копируем всё из builder (Python 3.8, pip, настройки)
COPY --from=builder /usr/local /usr/local
COPY --from=builder /opt/bitnami/python /opt/bitnami/python
COPY --from=builder /etc/alternatives /etc/alternatives

# Копируем переменные окружения
ENV PYSPARK_PYTHON=/usr/local/bin/python3.8 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.8 \
    SPARK_PYTHON=/usr/local/bin/python3.8 \
    SPARK_DRIVER_PYTHON=/usr/local/bin/python3.8 \
    PATH=/usr/local/bin:/usr/bin:/opt/bitnami/python/bin:$PATH

# Устанавливаем все потенциально необходимые runtime-зависимости в отдельном слое
RUN apt-get update && rm -rf /var/lib/apt/lists/* && \
    apt-get update && apt-get install -y --no-install-recommends \
        libffi8 \
        libssl3 \
        zlib1g \
        libbz2-1.0 \
        libreadline8 \
        libsqlite3-0 \
        libncurses6 \
        libncursesw6 \
        xz-utils \
        liblzma5 \
        libdb5.3 \
        libgdbm6 \
        libuuid1 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Проверяем, что python3 использует Python 3.8 и ключевые модули
RUN python3 --version && \
    /usr/local/bin/python3.8 -c "import ctypes; print('ctypes OK')" && \
    /usr/local/bin/python3.8 -c "import ssl; print('ssl OK')" && \
    /usr/local/bin/python3.8 -c "import sqlite3; print('sqlite3 OK')" && \
    /usr/local/bin/python3.8 -c "import zlib; print('zlib OK')"

# Возвращаемся к пользователю bitnami
USER 1001