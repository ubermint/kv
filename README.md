Дискове сховище пар ключів і значень на основі лог-структурованих хеш-таблиць.

Особливості:
- Низька затримка для читання та запису;
- Висока пропускна здатність;
- Легке резервне копіювання / відновлення;
- Простий і зрозумілий інтерфейс;
- Зберігає дані об'ємом набагато більшим, ніж оперативна пам'ять/

### Формат записів, для збережена пар ключ-значення на диску, виглядає так:

| CRC | timestamp | key_size | value_size | key | value |
|-----|-----------|----------|------------|-----|-------|

Перші 4 байти є 32-бітним цілим числом, що представляє CRC.
Наступні 4 байти є 32-бітним цілим числом, яке представляє мітку часу.
Наступні 8 байтів є двома 32-розрядними цілими числами, що представляють розмір ключа та розмір значення.
Решта байтів є нашим ключем і значенням.


---

Модуль `format` забезпечує функції кодування/декодування cеріалізації та десеріалізації записів у бінарний формат.

Модуль `storage` реалізує клас Storage, який представляє інтерфейс KV, керує ініціалізацією сховища, оновлює внутрішню таблицю пам'яті, виконує операції читання та запису у файли.

### Використання:
```go
var db storage.Storage

err := db.New("test")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

db.Set([]byte("idiot"), []byte("dostoevsky"))

val, err := db.Get([]byte("idiot"))
if err != nil {
    log.Println(err)
}

log.Println("Value: ", string(val))
```