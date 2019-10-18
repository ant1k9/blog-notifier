### Usage

```bash
>>> python blog_notifier.py --help
>>> python blog_notifier.py --migrate  # create sqlite3 database with empty tables
>>> python blog_notifier.py --crawl    # crawl new articles and send email to client
```

### Example configuration

```yaml
server:
    host: smtp.gmail.com
    port: 465

client:
    email: example@gmail.com
    password: examplepassword
```
