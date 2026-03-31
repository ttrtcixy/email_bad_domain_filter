# Email Filter

[Русская версия](./README.ru.md)

Minimal in-memory disposable email domain filter.

## Important

Works correctly **only with sorted domain files** (ascending lexicographic order).

## Env config

- `EMAIL_FILTER_FILE_URL` (required)
- `EMAIL_FILTER_FILE_UPDATE_DURATION` (required)
- `EMAIL_FILTER_UPDATE_TASK` (optional, default `true`)

## File format

One domain per line:

```text
10minutemail.com
mailinator.com
tempmail.org
```

## Example

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

## Validation examples

```go
f.IsDomainValid("mailinator.com") // false (blocked)
f.IsDomainValid("gmail.com")      // true (allowed)
```

## Notes

- Startup does initial fetch.
- Background refresh uses `ETag` (`If-None-Match`).
- Empty loaded list means every domain is treated as valid.
