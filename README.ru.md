# Email Filter

[English version](./README.md)

Минималистичный in-memory фильтр одноразовых email-доменов.

## Важно

Корректно работает **только с отсортированными файлами доменов** (по возрастанию, лексикографически).

## Конфиг через env

- `EMAIL_FILTER_FILE_URL` (обязательно)
- `EMAIL_FILTER_FILE_UPDATE_DURATION` (обязательно)
- `EMAIL_FILTER_UPDATE_TASK` (необязательно, по умолчанию `true`)

## Формат файла

Один домен на строку:

```text
10minutemail.com
mailinator.com
tempmail.org
```

## Пример

```go
cfg := &emailfilter.Config{
	UpdateTime: 10 * time.Minute,
	Url:        "https://example.com/disposable_domains.txt",
	UpdateTask: true,
}

f, err := emailfilter.New(ctx, log, cfg)
if err != nil {
	panic(err)
}
```

## Примеры проверки

```go
f.IsDomainValid("mailinator.com") // false (блокируется)
f.IsDomainValid("gmail.com")      // true (разрешен)
```

## Заметки

- При старте выполняется первичная загрузка.
- Фоновое обновление использует `ETag` (`If-None-Match`).
- Если список пустой, любые домены считаются валидными.
