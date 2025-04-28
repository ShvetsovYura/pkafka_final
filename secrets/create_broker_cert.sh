#!/bin/bash

# Пути по умолчанию (можно изменить)
DEFAULT_CA_CRT="./ca/ca.crt"
DEFAULT_CA_KEY="./ca/ca.key"
DEFAULT_CA_PEM="./ca/ca.pem"

# Запрашиваем пути с подсказками о значениях по умолчанию
read -p "Введите путь к директории для файлов (например, ./creds): " CREDS_DIR
read -p "Введите путь к файлу конфигурации CSR (например, ./creds/broker.cnf): " CSR_CONFIG
read -p "Введите путь к CA-сертификату [по умолчанию: $DEFAULT_CA_CRT]: " CA_CRT
read -p "Введите путь к CA-ключу [по умолчанию: $DEFAULT_CA_KEY]: " CA_KEY
read -p "Введите путь к CA в формате PEM [по умолчанию: $DEFAULT_CA_PEM]: " CA_PEM

# Подставляем значения по умолчанию, если ввод пустой
CA_CRT=${CA_CRT:-$DEFAULT_CA_CRT}
CA_KEY=${CA_KEY:-$DEFAULT_CA_KEY}
CA_PEM=${CA_PEM:-$DEFAULT_CA_PEM}

# Запрашиваем или генерируем пароли
read -s -p "Введите пароль для keystore (оставьте пустым для генерации): " KEYSTORE_PASS
echo
read -s -p "Введите пароль для truststore (оставьте пустым для генерации): " TRUSTSTORE_PASS
echo

# Генерация паролей, если не введены
generate_password() {
  openssl rand -base64 16 | tr -d '/+=' | cut -c1-12
}

[ -z "$KEYSTORE_PASS" ] && KEYSTORE_PASS=$(generate_password)
[ -z "$TRUSTSTORE_PASS" ] && TRUSTSTORE_PASS=$(generate_password)

# Создаем директорию
mkdir -p "$CREDS_DIR"

# 1. Генерация ключа и CSR (без пароля, так как используем -nodes)
echo "Генерация ключа и CSR..."
openssl req -new -newkey rsa:2048 \
  -keyout "$CREDS_DIR/broker.key" \
  -out "$CREDS_DIR/broker.csr" \
  -config "$CSR_CONFIG" \
  -nodes

# 2. Подписание сертификата
echo "Подписание сертификата CA..."
openssl x509 -req -days 3650 \
  -in "$CREDS_DIR/broker.csr" \
  -CA "$CA_CRT" \
  -CAkey "$CA_KEY" \
  -CAcreateserial \
  -out "$CREDS_DIR/broker.crt" \
  -extfile "$CSR_CONFIG" \
  -extensions v3_req

# 3. Создание PKCS12-архива (используем пароль для ключа)
echo "Создание PKCS12 архива..."
openssl pkcs12 -export \
  -in "$CREDS_DIR/broker.crt" \
  -inkey "$CREDS_DIR/broker.key" \
  -chain -CAfile "$CA_PEM" \
  -name broker \
  -out "$CREDS_DIR/broker.p12" \
  -password pass:"$KEYSTORE_PASS"

# 4. Импорт в keystore (пароль keystore)
echo "Создание keystore..."
keytool -importkeystore \
  -deststorepass "$KEYSTORE_PASS" \
  -destkeystore "$CREDS_DIR/keystore.pkcs12" \
  -srckeystore "$CREDS_DIR/broker.p12" \
  -srcstorepass "$KEYSTORE_PASS" \
  -deststoretype PKCS12 \
  -srcstoretype PKCS12 \
  -noprompt

# 5. Создание truststore (отдельный пароль)
echo "Создание truststore..."
keytool -import \
  -file "$CA_CRT" \
  -alias ca \
  -keystore "$CREDS_DIR/truststore.jks" \
  -storepass "$TRUSTSTORE_PASS" \
  -noprompt

# 6. Сохраняем пароли в разные файлы
echo "Сохранение паролей..."
echo "$KEYSTORE_PASS" > "$CREDS_DIR/ssl_key_creds"          # Пароль от приватного ключа
echo "$KEYSTORE_PASS" > "$CREDS_DIR/keystore_creds"    # Пароль от keystore
echo "$TRUSTSTORE_PASS" > "$CREDS_DIR/truststore_creds" # Пароль от truststore

# 7. Защищаем файлы
chmod 600 "$CREDS_DIR"/*_creds
chmod 600 "$CREDS_DIR"/*.p12
chmod 600 "$CREDS_DIR"/*.jks
chmod 600 "$CREDS_DIR"/*.key

# Выводим информацию
echo -e "\n\033[32mГотово!\033[0m"
echo -e "\nФайлы сохранены в: \033[34m$CREDS_DIR\033[0m"