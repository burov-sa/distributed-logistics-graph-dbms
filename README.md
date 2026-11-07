# Distributed Graph DBMS for AnyLogic Logistics Simulation

Проект по разработке легковесной распределенной графовой СУБД на Java по дисциплине **Параллельная и распределенная обработка данных** в университете ИТМО. В ходе разработки система оптимизируется для решения задач мультикритериальной маршрутизации агентов в среде имитационного моделирования **AnyLogic**.

**Группа:** ПИРСБД

**Студент:** Буров С.А. (563821)

**Текущий статус:** Этап 1 (Проектирование интерфейсов).

---

## 1. Архитектурный кейс

Для решения аналитических, оптимизационных и прогностических задач разрабатываются имитационные модели транспортной логистики в среде AnyLogic. Имитационная модель представляет собой Java приложения, фундаментом которой являются библиотеки среды AnyLogic. При моделировании крупных транспортных логистических сетей для нахождения расстояний используются встроенные ГИС-карты и тяжелые вычисления путей для тысяч агентов перегружают CPU при моделировании. Предлагается разработать СУБД оптимизированную под решения задач транспортной логистики, которая сможет вынести эти вычисления в распределенный кластер.

### Предлагаемая структура данных графа
* **Узлы (Nodes):** Логистические объекты.
  * Свойства: **id**, **label**, **attributes** (метаданные, например, вместимость или количество ворот приёмки/отгрузки).
* **Связи (Edges):** Транспортные маршруты (**ROUTE_TO**).
  * Свойства (метрики веса): Задаваемые пользователем, уже имеемые по умолчанию: **time** (часы), **distance** (километры), **cost** (рубли/условные единциы).

---

## 2. План реализации учебного проекта

* [x] **Этап 1: Обобщённая архитектура, концепция, основные классы и интерфейсы**
* [x] **Этап 2: Ядро СУБД** - Разработка методов сохранения и получения данных на одной ноде.
* [x] **Этап 3: Сетевой слой и Клиент** — Разработка сервера БД на основе TCP socket + length-prefixed text и легковесного Java-клиента (.jar) для импорта в AnyLogic.
* [x] **Этап 4: Шардирование графа** — Горизонтальное масштабирование при нехватке памяти. Распределение узлов по хэшу ID между независимыми нодами СУБД.
* [ ] **Этап 5: Репликация Master-Slave** — Реализация отказоустойчивости. Запись идет на Master ноду, чтение тяжелых маршрутов агентами AnyLogic балансируется по Slave-нодам.



---

## 3. Архитектура ядра (классы и интерфейсы)

На диаграмме приведены слои модели разрабатываемой графовой базы данных и их взаимосвязь.

![клиентский слой --> сетевой слой --> слой маршрутизации --> слой хранения --> модельный слой](schema-Stack%20Diagram.drawio.png)


Архитектура строится на принципах слабой связанности (**Low Coupling**). К примеру, модуль хранения (**GraphStorage**) абстрагирован от алгоритмов поиска путей (**PathFinder**). На представленной ниже диаграмме классов, классы у которых не обозначены параметры и/или методы на текущем этапе являются заглушками, которые будут дорабатываться на последующих этапах. В том числе будут добавляться дополнительные интерфейсы и их имплементация для реализации механзимов репликации и шардирования. Оригиналы схем в файле **schema.drawio (редактор draw.io)** в корневом каталоге проекта.

![диаграмма классов](schema-Class%20Diagram.drawio.png)

---

## 4. Файловая структура проекта

```text
distributed-logistics-graph-dbms/
├── .gitignore
├── pom.xml 
├── readme.md
└── src/
    ├── main/
    │   └── java/
    │       └── ru/itmo/graphdb/
    │           ├── client/ 
    │           │   ├── CommandExecutor.java                # Контракт исполнителя команд (абстракция для сети и шардирования)
    │           │   ├── CommandTranslator.java              # Транслятор команд СУБД 
    │           │   └── GraphDBClient.java                  # API-клиент для агентов AnyLogic
    │           ├── model/     
    │           │   ├── CustomMetric.java                   # Дополнительные пользовательские метрики рёбер
    │           │   ├── DefaultMetric.java                  # Метрики рёбер по умолчанию для транспортной логистики (DISTANCE, TIME, COST)
    │           │   ├── Edge.java                           # Сущность ребра (маршрута)
    │           │   ├── Metric.java                         # Метрика рёбер
    │           │   ├── MetricRegistry.java                 # Регистр используемых меток рёбер
    │           │   └── Node.java                           # Сущность вершины (логистический объект)
    │           ├── network/      
    │           │   ├── protocol/  
    │           │   │   ├── FrameType.java                  # Тип кадра протокола (REQ, RESP, PUSH)
    │           │   │   ├── Frame.java                      # Один кадр протокола: тип + payload
    │           │   │   └── Frames.java                     # Утилита сериализации/десериализации кадров
    │           │   ├── CommandExecutorFactory.java         # Фабрика исполнителей команд на соединение
    │           │   ├── ClientHandler.java                  # Обработчик одного сетевого соединения
    │           │   └── DBServer.java                       # Точка входа и запуска сервера БД, управление жизненным циклом         
    │           ├── router/  
    │           │   ├── ShardedGraphStorage.java            # Реализация GraphStorage поверх шардов (маршрутизация getNode/getEdgesFrom)
    │           │   ├── RouterCommandTranslator.java        # Транслятор команд на Router (маршрутизация и fan-out)
    │           │   └── RouterServer.java                   # Точка входа Router-ноды (композиция DBServer + ShardRouter + push SHARD_MAP)
    │           ├── routing/  
    │           │   ├── PathFinder.java                     # Интерфейс поиска путей в графе 
    │           │   └── SimpleDijkstraPathFinder.java       # Реализация поиска путей в графе Дейкстрой
    │           ├── sharding/
    │           │   ├── ShardCommandTranslator.java         # Транслятор команд на шард-ноде (внутренние _-команды)
    │           │   ├── ShardEndpoint.java                  # Описание шарда (shardId, host, port)
    │           │   └── ShardRouter.java                    # Раскладка узлов по шардам (hashing) и топология
    │           └── storage/  
    │               ├── GraphStorage.java                   # Базовый интерфейс хранилища
    │               └── InMemoryGraphStorage.java           # Локальная реализация  
    └── test/
        └── java/
            └── ru/itmo/graphdb/
                ├── client/ 
                │   ├── CommandTranslatorTest.java          # Unit-тесты транслятора команд СУБД 
                │   ├── ConsoleRepl.java                    # Тестовый REPL для ввода команд
                │   ├── DemoClientTest.java                 # Тестовая БД
                │   └── GraphDBClientTest.java              # Тесты TCP-клиента (включая протокол и push)
                ├── network/
                │   └── DBServerTest.java                   # Тесты сетевого слоя и жизненного цикла сервера
                ├── router/
                │   ├── PushProtocolIntegrationTest.java    # Интеграционные тесты push-уведомлений (SHARD_MAP)
                │   ├── ShardedClusterIntegrationTest.java  # Интеграционные тесты маршрутизации команд через Router
                │   ├── ShardedClusterRobustnessIntegrationTest.java # Тесты отказоустойчивости кластера и конкурентной нагрузки
                │   └── ShardedGridPathfindingIntegrationTest.java   # Интеграционные тесты PATH_EDGE (в т.ч. через границы шардов)
                ├── routing/     
                │   └── SimpleDijkstraPathFinderTest.java   # Unit-тесты нахождения кратчайшего пути алгоритмом Дейкстры
                └── sharding/
                    ├── ShardCommandTranslatorTest.java     # Unit-тесты внутренних _-команд шард-ноды
                    ├── ShardedClusterLifecycleIntegrationTest.java # Тесты SAVE_DB/READ_DB на шардированном кластере
                    └── ShardRouterTest.java                # Unit-тесты раскладки узлов по шардам     
```
                    
---
## 4. Алфавит и структура команд для работы с СУБД

| Команда                                                                                                 | Операция                       |
| ------------------------------------------------------------------------------------------------------- | ------------------------------ |
| CREATE_DB &lt;name_db&gt;                                                                               | Создание базы данных           |
| READ_DB &lt;name_db&gt;                                                                                 | Чтение базы данных с диска     |
| SAVE_DB &lt;name_db&gt;                                                                                 | Сохранине базы данных на диск  |
| ADD_METRIC &lt;metric&gt;                                                                               | Добавление новой метрики       |
| DELETE_METRIC &lt;metric&gt;                                                                            | Удаление метрики               |
| PRINT_METRICS                                                                                           | Вывод имеемых метрик           |
| ADD_NODE &lt;name_node&gt;                                                                              | Добавление вершины             |
| SET_PARAMETER &lt;name_node&gt; &lt;name_parameter&gt; &lt;value_parameter&gt;                          | Задание параметра вершины      |
| DELETE_NODE &lt;name_node&gt;                                                                           | Удаление вершины               |
| ADD_EDGE &lt;name_node_source&gt; &lt;name_node_destination&gt; &lt;metric&gt; &lt;metric_value&gt;     | Добавление связи               |
| PRINT_EDGES &lt;name_node&gt;                                                                           | Вывод связей вершины           |
| PRINT_EDGES &lt;name_node&gt; &lt;name_node&gt;                                                         | Вывод связей пары вершин       |
| DELETE_EDGE <id_edge>                                                                                   | Удаление связи                 |
| PATH_EDGE &lt;name_node_source&gt; &lt;name_node_destination&gt; &lt;metric&gt;                         | Кратчайший путь между вершинами|

>[!NOTE]
>В тестовых классах для тестирования ввода команд разработан REPL. Он реализован в основном методе класса `test.ConsoleRepl`
> 
---
## 5. Архитектура ноды СУБД
| Компонент | Создаётся | Разделяется между клиентами | Функция |
|---|---|---|---|
| `DBServer` | 1 раз | — | Управление жизненным циклом сервера (start(port)/stop(), открытие и закрытие ServerSocket, запуск и сотановка accept-потока), процессы accept-loop, создаёт пул на 8 потоков с внутренней неограниченной очередью для обработки подключений |
| `GraphStorage` | 1 раз | **да** | Создаёт и хранит граф СУБД - один на ноду |
| `MetricRegistry` | 1 раз | **да** | Использует глобальный для СУБД метрики |
| `PathFinder` | 1 раз | **да** | Stateless, безопасен |
| `ServerSocket` | 1 раз | — | Один порт |
| `ExecutorService` | 1 раз | — | Ограничивает число потоков обработчиков заданным параметром (сейчас 8) для безопасности|
| `activeClients` | 1 раз | — | Потокобезопасно хранит клиентов Set<Socket>, позволяет безопасно их закрывать (для принудительного `close()` в `stop()`) |
| `acceptThread` | 1 раз | — | Выделенный acceptor, крутит accept() и передаёт содинение в пул.|
| `ClientHandler` | на соединение | **нет** | Свой `readUTF`/`writeUTF` цикл (прочитать команду -> выполнить -> записать ответ). Живёт ровно столько, сколько TCP-соединение|
| `CommandTranslator` | на соединение | **нет** | Изоляция `currentDbName`, часть сессии клиента. Создаётся в acceptLoop  перед submit, умирает вместе с ClientHandler.|
| `Socket` / `DataIn` / `DataOut` | на соединение | **нет** | Один канал = один диалог |

### 5.1 Структура ноды
![структура ноды](schema-Structure%20Node.drawio.png)
### 5.2 Состояний сервера (`DBServer`)
![состояния сервера](schema-DB%20Server%20State.drawio.png)
### 5.3 Детализация обработчика подключения клиента (`ClientHandler`)
![поток клиента](schema-Flow%20ClientHandle.drawio.png)
### 5.4 Детализация обработчика подключения клиента (`ClientHandler`)
![диаграмма взаимодействия с нодой](schema-%20Sequence%20Diagram%20ClientNode.drawio.png)

## 6. Шардирование графа

> Раздел описывает Этап 4. Архитектурная цель - горизонтальное масштабирование
> при нехватке памяти на одной ноде. Ключевое ограничение: **клиент ничего не
> знает о шардах**, используется разработанный ранее `SimpleDijkstraPathFinder`,
> который не изменился при шардировании.

### 6.1. Топология
![schema-Топология шардирования.drawio.png](schema-%D0%A2%D0%BE%D0%BF%D0%BE%D0%BB%D0%BE%D0%B3%D0%B8%D1%8F%20%D1%88%D0%B0%D1%80%D0%B4%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F.drawio.png)

* **Клиент** подключается только к Router. `new GraphDBClient(routerHost, routerPort)`
  — тот же API, что и на Этапе 3.
* **Router** держит по одному `GraphDBClient` на каждый шард. Маршрутизация
  клиентских команд — `RouterCommandTranslator`.
* **Шард** — это `DBServer` с фабрикой на `ShardCommandTranslator`. Сервер
  ничего не знает про шардирование: он просто использует тот `CommandExecutor`,
  который ему передали при запуске.

### 6.2. Модель данных

Правила раскладки (реализованы в `ShardRouter`):

* **Узел `X` живёт на шарде `h(X) = floorMod(hashCode(X), N)`.**
* **Ребро `A → B` живёт на шарде своего source `h(A)`** — рядом с вершиной,
  из которой оно исходит (adjacency list).

Следствия:

* `getEdgesFrom(node)` — **всегда один сетевой вызов** на шард `h(node.id)`,
  без fan-out. Это ключ к тому, что `SimpleDijkstraPathFinder` продолжает
  работать через `GraphStorage`-абстракцию без изменений.
* `ADD_EDGE src dst` всегда маршрутизируется на `h(src)`; `dst` может лежать
  где угодно.
* **Заглушки (foreign stubs).** Если `dst` лежит на другом шарде, source-шард
  создаёт «лёгкий» `Node(dst)` с атрибутом `__foreign__=true` — только чтобы
  удовлетворить инвариант `InMemoryGraphStorage.addEdge`. При маршрутизации
  через Router `getNode(dst)` всё равно уйдёт на домашний шард `dst`, так
  что эта заглушка клиенту не видна.
* **Удаление узла — fan-out.** `DELETE_NODE X` рассылает `_DELETE_EDGES_TO X`
  на все шарды: на домашнем `InMemoryGraphStorage.deleteNode` снимет сам узел
  и его исходящие рёбра; на остальных — заглушки и случайно залетевшие
  входящие рёбра.

Ограничения:
* `hashCode % N` — при добавлении/удалении шарда все ключи переезжают.
  Consistent hashing и rebalancing — future work.
* Топология фиксирована на старте Router. Broadcast нового `SHARD_MAP` уже
  подключённым клиентам (`DBServer.broadcastPush`) — инфраструктура готова,
  политика — future work.

### 6.3. Протокол (двунаправленный сокет)

Изначально протокол был «один UTF-туда — один UTF-обратно». Сокет
двунаправленный, и на Этапе 4 (репликация) серверу понадобится отправлять
клиенту асинхронные уведомления — события `MASTER_CHANGED`,
`SHARD_UNAVAILABLE` и т.п. Поэтому протокол расширен минимально — **типом
кадра**:

```text
[1 байт: FrameType.code] [2+N байт: writeUTF(payload)]

  REQ  = 1   клиент → сервер: команда
  RESP = 2   сервер → клиент: ответ на конкретный REQ
  PUSH = 3   сервер → клиент: асинхронное уведомление (в любой момент сеанса)
```

Классы: `network/protocol/FrameType.java`, `Frame.java`, `Frames.java`.

**Почему это безопасно при `synchronized execute()` на клиенте.** В полёте
всегда ровно один `REQ` на клиента (это гарантирует `GraphDBClient.execute`).
Значит, ответ — первый `RESP`-кадр после отправки, а всё, что прилетает до
него, — `PUSH`. Клиент в цикле `doExecute` разбирает кадры и передаёт
`PUSH` в листенер; листенер, установленный после первого `execute`, получает
все накопленные push'и в порядке поступления.

**Push-события Этапа 5.** При подключении клиента Router отправляет
`SHARD_MAP <id>=<host>:<port>;…` через `DBServer.setOnClientConnected` →
`ClientHandler.sendPush(...)`. Это использует тот же механизм, что и будущие
события репликации — для Этапа 4 остаётся только вызвать `broadcastPush`
с нужным текстом.

Дополнительно на обеих сторонах сокета включён `setTcpNoDelay(true)` и
потоки обёрнуты в `BufferedInputStream`/`BufferedOutputStream`:
* `Nagle + Delayed ACK` на request-response протоколе даёт ~40 мс на
  round-trip;
* `writeByte(type) + writeUTF(payload)` — 2–4 сисколла; буферизация
  сливает их в один TCP-сегмент.

При `PATH_EDGE` через границы шардов это критично: без `NODELAY` один
запрос с 10 round-trip'ами упирается в 10-секундный `readTimeout` клиента.

### 6.4. Внутренние команды шарда

`ShardCommandTranslator` расширяет `CommandTranslator`, добавляя команды
с префиксом `_`. Они не предназначены для пользователя и вызываются только
Router'ом:

| Команда | Назначение |
|---|---|
| `_GET_NODE <id>` | Вернуть `NODE\t<id>\t<attrs>` или `NOT_FOUND` |
| `_GET_EDGES_FROM <id>` | Список исходящих рёбер (по строке на ребро, формат save-файла); `NOT_FOUND` если узла нет, `NO_EDGES` если рёбер нет |
| `_DELETE_EDGES_TO <id>` | Удалить все входящие рёбра на `<id>` и локальную заглушку (для fan-out при `DELETE_NODE`) |
| `_ADD_EDGE_RAW <src> <dst> <m> <v>` | Добавить ребро без требования, чтобы `<dst>` был реальным узлом этого шарда; при отсутствии создаёт «заглушку» `dst` |
| `_CLEAR` | Очистить хранилище шарда (для `CREATE_DB`) |

Все прочие команды делегируются обычному `CommandTranslator` — шард остаётся
самодостаточной нодой.

### 6.5. Маршрутизация клиентских команд (Router)

`RouterCommandTranslator` реализует `CommandExecutor` и живёт на Router,
по экземпляру на клиентское соединение (изолирует `currentDbName`). Общие
компоненты — `ShardRouter`, `Map<Integer, GraphDBClient>`, `ShardedGraphStorage`,
`MetricRegistry`, `PathFinder` — шарятся.

Таблица маршрутизации:

| Команда клиента | Куда идёт |
|---|---|
| `ADD_NODE id` | `shard[h(id)] : ADD_NODE id` |
| `SET_PARAMETER id k v` | `shard[h(id)] : SET_PARAMETER …` |
| `DELETE_NODE id` | fan-out `_DELETE_EDGES_TO id` на все шарды |
| `ADD_EDGE src dst m v` | `shard[h(src)] : _ADD_EDGE_RAW …` |
| `PRINT_EDGES node` | `shard[h(node)] : PRINT_EDGES …` |
| `PRINT_EDGES src dst` | `shard[h(src)] : PRINT_EDGES …` |
| `DELETE_EDGE src->dst` | `shard[h(src)] : DELETE_EDGE …` |
| `PATH_EDGE src dst m` | **локально** через `ShardedGraphStorage` + `PathFinder` |
| `ADD_METRIC` / `DELETE_METRIC` | локальный `MetricRegistry` + fan-out всем шардам |
| `PRINT_METRICS` | локальный `MetricRegistry` (Router-авторитетен) |
| `CREATE_DB name` | fan-out `_CLEAR` всем шардам |
| `SAVE_DB name` | fan-out `SAVE_DB <name>.shard<id>` |
| `READ_DB name` | fan-out `READ_DB <name>.shard<id>` |

`SAVE_DB`/`READ_DB` используют суффикс `.<name>.shard<id>`: один логический
дамп БД раскладывается по файлам, по файлу на шард. `CREATE_DB` очищает все
шарды и фиксирует имя в сессии.

### 6.6. `PATH_EDGE` через границы шардов

`SimpleDijkstraPathFinder` принимает `GraphStorage` и обращается только к
`getNode(id)` и `getEdgesFrom(node)`. На Router вместо `InMemoryGraphStorage`
подставляется `ShardedGraphStorage implements GraphStorage`:

* `getNode(id)` — `_GET_NODE` на `shard[h(id)]`, парсинг `NODE\t<id>\t<attrs>`;
* `getEdgesFrom(node)` — `_GET_EDGES_FROM` на `shard[h(node.id)]`, парсинг
  многострочного ответа `EDGE\t<id>\t<src>\t<dst>\t<weights>`;
* `getEdgesBetween(src, dst)` — `getEdgesFrom(src)` + фильтр по `dst`;
* мутирующие методы — `UnsupportedOperationException` (запись только через
  `RouterCommandTranslator`).

Заглушки destination (`Node(dstId)` без атрибутов) для Dijkstra достаточны:
алгоритм использует только `getId()`. Это убирает лишний round-trip на
каждое ребро.

**Результат:** `PathFinder` и `SimpleDijkstraPathFinder` не менялись
вообще. Это прямое следствие принципа Low Coupling из README: модуль
маршрутизации не зависит от того, один это узел или кластер.

Известный трейд-офф: `PATH_EDGE` на маршруте из K вершин делает ~2K сетевых
вызовов (`getNode` + `getEdgesFrom`). Для демо это ок; очевидная оптимизация —
`CachingGraphStorage implements GraphStorage` как декоратор на время одного
запроса. `PathFinder` снова не трогается.

### 6.7. Push-канал: `SHARD_MAP` и задел на Этап 4

На коннект Router пушит клиенту топологию:

```text
PUSH SHARD_MAP 0=localhost:7000;1=localhost:7001;2=localhost:7002
```

Клиент получает событие через `GraphDBClient.setPushListener(Consumer<String>)`.
Push'и, пришедшие до установки листенера, буферизуются и выдаются при подписке.

Для Этапа 4 тот же канал используется под события:
* `SHARD_UNAVAILABLE shard=X` — Router не смог достучаться до шарда;
* `MASTER_CHANGED shard=X new_master=host:port` — при промоушене Slave в
  Master после падения.

Никаких новых механизмов не потребуется — только вызов `DBServer.broadcastPush`
из места, где произошло событие.

### 6.8. Ограничения и trade-offs

| Ограничение | Причина | Путь улучшения |
|---|---|---|
| `hashCode % N` | Простота; детерминированность | Consistent hashing, range-based sharding |
| Топология фиксирована на старте | Не реализована политика rebalance | Инфраструктура broadcast нового `SHARD_MAP` готова |
| `ShardedGraphStorage` шарит `GraphDBClient` на шард | `execute()` — `synchronized` | Per-connection пул клиентов на шард |
| `PATH_EDGE` делает O(K) round-trip'ов | Нет кэша промежуточных `getEdgesFrom` | `CachingGraphStorage`-декоратор на время запроса |
| Клиент может подключиться к шарду напрямую | Шард не отделяет клиентский порт от router-порта | Отдельный порт для `_`-команд, firewall / auth |
| `ShardedGraphStorage.getAllNodes()` не реализован | Dijkstra не требует | Добавить `_LIST_NODES` в `ShardCommandTranslator` |
| `DELETE_NODE` — O(N_shards) | Нет глобального индекса «кто ссылается на X» | Reverse-index, кэш входящих рёбер на шарде |

### 6.9. Тестирование

**Unit-тесты.**
* `ShardRouterTest` — детерминированность `h(id)`, распределение по шардам,
  wire-формат `SHARD_MAP`, валидация `shardId`/`port`, round-trip через
  `asText`/`fromText`.
* `ShardCommandTranslatorTest` — все `_`-команды, включая:
  * создание foreign stub при `_ADD_EDGE_RAW` с чужим `dst`;
  * запрет `_ADD_EDGE_RAW` из foreign stub;
  * `_DELETE_EDGES_TO` снимает и рёбра, и заглушки;
  * делегирование обычных команд в `CommandTranslator`;
  * регистрозависимость `_`-команд.

**Интеграционные тесты.**
* `ShardedClusterIntegrationTest` — базовая маршрутизация: `ADD_NODE` идёт
  на домашний шард, `PATH_EDGE` через границу шардов, кросс-шардовый
  `DELETE_NODE`, fan-out метрик, `CREATE_DB` очищает все шарды.
* `ShardedGridPathfindingIntegrationTest` — «двумерный» тест: сетка 4×4 на
  3 шардах, все пары вершин сверяются с эталоном (локальный `InMemoryGraphStorage`
  + `SimpleDijkstraPathFinder`). Дополнительно:
  * неравномерные веса — тоже сверка с эталоном;
  * форсированный зигзаг s0→s1→s2→s0, каждое ребро пересекает границу
    шардов;
  * «одна пара — два маршрута»: оптимальный по distance ≠ оптимальный
    по time.
* `ShardedClusterRobustnessIntegrationTest` — отказоустойчивость:
  * `PATH_EDGE` через мёртвый шард → `ERROR:`, Router жив;
  * путь целиком на живом шарде работает, несмотря на смерть другого;
  * восстановление после перезапуска шарда;
  * конкурентная нагрузка 4 потока × 8 `PATH_EDGE` = 32 запроса, 0 ошибок,
    у каждого клиента ровно один `SHARD_MAP` (нет реконнектов).
* `PushProtocolIntegrationTest` — протокол:
  * listener до `execute` — `SHARD_MAP` доставлен, `RESP` не съеден;
  * listener после `execute` — буферизованный `SHARD_MAP` доставлен;
  * push не ломает поток 20 подряд `execute`;
  * два клиента получают одинаковый `SHARD_MAP`.
* `ShardedClusterLifecycleIntegrationTest` — `SAVE_DB` создаёт по файлу
  на шард, `CREATE_DB` очищает всё, `READ_DB` восстанавливает кросс-шардовый
  путь с обоими весами; `READ_DB` несуществующего → `ERROR:`.

### 6.10. Запуск кластера

```bash
# Шард 0
java -cp target/classes ru.itmo.graphdb.network.DBServer --port 7000 --shard-id 0

# Шард 1
java -cp target/classes ru.itmo.graphdb.network.DBServer --port 7001 --shard-id 1

# Router
java -cp target/classes ru.itmo.graphdb.router.RouterServer --port 5555 \
    --shard 0=localhost:7000 --shard 1=localhost:7001
```

Без `--shard-id` `DBServer.main` запускает standalone-ноду (Этап 3) — старый
сценарий остаётся рабочим. Router CLI принимает повторяемый флаг `--shard`
формата `<shardId>=<host>:<port>`.

### 6.11. Дорожная карта: Этап 4 (репликация)

Репликация накладывается на шардирование без переделок как «per-shard
ReplicaSet»:

* Каждый шард — независимая группа master + slaves со своим
  `ReplicationLog` (WAL, `seq` + команда) и своим `ReplicaManager`.
* `RouterCommandTranslator` остаётся единственной точкой маршрутизации:
  пишет в master шарда, читает — со slave (или с master для read-your-writes).
* Push-канал (`broadcastPush` / `SHARD_MAP`) уже готов: события
  `MASTER_CHANGED`, `SHARD_UNAVAILABLE` летят к клиентам через тот же
  `ClientHandler.sendPush`.
* `ShardedGraphStorage` не меняется: ему всё равно, master или slave
  ответил на `_GET_NODE`/`_GET_EDGES_FROM`.

Выбор WAL (а не push-команд или декоратора над `GraphStorage`) обоснован
тем, что он естественно ложится на per-shard модель: один журнал на шард,
свой `lastSeq` у каждого Slave, переподключение и догон — через `REPL_SYNC`.