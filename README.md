На основе анализа предоставленного кода, это **MUMPS-like Database (MUMPS_BD)** - полнофункциональная NoSQL база данных с поддержкой MUMPS-подобного синтаксиса и расширенными возможностями.

## Общее описание проекта

**MUMPS_BD** - это Java-реализация базы данных, вдохновленная языком MUMPS (Massachusetts General Hospital Utility Multi-Programming System), с современными расширениями включая векторные embedding, семантический поиск, репликацию и шардинг.

### Ключевые особенности:

1. **MUMPS-совместимый синтаксис** - поддержка команд SET, GET, KILL, WRITE, $ORDER
2. **Древовидная структура данных** - глобалы с подстрочными индексами
3. **Векторные embedding** - интеграция с Ollama для семантического поиска
4. **Распределенная архитектура** - репликация и шардинг
5. **Многопользовательский режим** - аутентификация и авторизация
6. **Транзакции** - ACID-совместимые транзакции
7. **Персистентность** - снапшоты и AOF (Append-Only File)
8. **Веб-интерфейс** - современный web-based терминал
9. **Консольный и сетевой режимы** - socket server + CLI

## Архитектурные компоненты

### 1. Ядро базы данных (`Database`, `TreeNode`)
```java
// Иерархическое хранение данных
database.set("^Patient", "John Doe", "name");
database.set("^Patient", 35, "age");
database.set("^Patient", "A+", "blood", "type");

// Получение данных
Object name = database.get("^Patient", "name"); // "John Doe"
```

### 2. Система команд
```java
// MUMPS-команды
SET ^Patient("name")="John Doe"
GET ^Patient("name")     // "John Doe"
KILL ^Patient("age")
WRITE "Hello, ",^Patient("name")
ZW ^Patient              // Просмотр всего глобала
```

### 3. Векторный поиск
```java
// Семантический поиск
List<VectorSearchResult> results = database.similaritySearch("heart attack", 10);

// Точный поиск  
List<VectorSearchResult> exactResults = database.exactSearch("diabetes");
```

### 4. Безопасность
```java
// Аутентификация
securityManager.authenticate("admin", "password");

// Проверка прав
securityManager.checkGlobalPermission(user, "^Patient", Permission.READ);
```

## Примеры использования

### 1. Базовые операции с данными
```java
Database db = new Database();

// Установка значений
db.set("^Patient", "John Smith", 1, "name");
db.set("^Patient", 45, 1, "age"); 
db.set("^Patient", "Male", 1, "gender");

// Получение значений
String name = (String) db.get("^Patient", 1, "name");
Integer age = (Integer) db.get("^Patient", 1, "age");

// Запрос с глубиной
List<QueryResult> results = db.query("^Patient", new Object[]{1}, 2);
```

### 2. Работа с транзакциями
```java
// Начало транзакции
Transaction tx = db.beginTransaction();

try {
    tx.set("^Account", "balance", 1000.0);
    tx.set("^Audit", "transaction", "withdrawal");
    
    // Коммит при успехе
    db.commitTransaction(tx);
} catch (Exception e) {
    // Откат при ошибке
    db.rollbackTransaction();
}
```

### 3. Семантический поиск
```java
// Включение auto-embedding
db.setAutoEmbeddingEnabled(true);

// Сохранение данных с автоматическим созданием embedding
db.set("^MedicalNotes", 
       "Patient shows symptoms of myocardial infarction", 
       "patient1", "notes");

// Поиск похожих записей
List<VectorSearchResult> similar = db.similaritySearch("heart attack symptoms", 5);

for (VectorSearchResult result : similar) {
    System.out.printf("Similarity: %.4f - %s%n", 
                     result.getSimilarity(), 
                     result.getValue());
}
```

### 4. Консольное использование
```bash
# Запуск сервера
java -jar mumpsdb.jar --socket

# Подключение через telnet
telnet localhost 9090

MUMPS> SET ^Test="Hello World"
OK

MUMPS> GET ^Test  
"Hello World"

MUMPS> SET ^Patient(1,"name")="John Doe"
OK

MUMPS> SET ^Patient(1,"age")=35
OK

MUMPS> ZW ^Patient
^Patient(1,"name")="John Doe"
^Patient(1,"age")=35

MUMPS> SIMSEARCH "heart problems" TOP 5
```

### 5. Веб-интерфейс
```bash
# Запуск с веб-сервером
java -jar mumpsdb.jar --both
```
Открыть http://localhost:8080 для доступа к веб-терминалу.

### 6. Работа с индексами
```java
// Создание составного индекса
db.createCompositeIndex("patient_name_age", 
                       new String[]{"name", "age"}, 
                       CompositeIndex.IndexType.HASH);

// Поиск по индексу
List<IndexResult> indexedResults = db.searchWithIndex("patient_name_age", 
                                                     new Object[]{"John", 35});
```

### 7. Репликация и бэкапы
```java
// Добавление реплики
replicationManager.addReplica("node2", "192.168.1.2", 9090);

// Создание бэкапа
String backupId = db.createFullBackup();

// Восстановление из бэкапа
db.restoreFromBackup(backupId);
```

## Конфигурация

Файл `application.properties`:
```properties
# Сервер
server.port=9090
server.host=localhost

# База данных
database.auto.embedding.enabled=true
database.query.default.depth=1

# Embedding (Ollama)
rag.embedding.model=all-minilm:22m
rag.embedding.host=localhost
rag.embedding.server.port=11434

# Персистентность
persistence.snapshot.file=database.snapshot
persistence.aof.file=commands.aof
```

## Запуск приложения

### Различные режимы:
```bash
# Только socket сервер
java -jar mumpsdb.jar --socket

# Только консольный режим  
java -jar mumpsdb.jar --console

# Оба режима (по умолчанию)
java -jar mumpsdb.jar --both

# С бенчмарком при старте
java -jar mumpsdb.jar --both -Dbenchmark.on.startup=true
```

## Преимущества системы

1. **Производительность** - оптимизированное дерево узлов, кэширование, индексы
2. **Масштабируемость** - шардинг и репликация
3. **Гибкость поиска** - точный, семантический и гибридный поиск
4. **Надежность** - транзакции, бэкапы, персистентность
5. **Безопасность** - многопользовательский доступ с правами
6. **Совместимость** - MUMPS-подобный синтаксис для медицинских систем

Этот проект представляет собой современную переработку концепций MUMPS с добавлением AI-возможностей и облачной архитектуры.