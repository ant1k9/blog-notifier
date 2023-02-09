## Blog Notifier

This scripts helps you to add to your watchlist sites with list of updating articles.

Usually sites have typical structure for representing articles or blogposts.

You can explore the one you want.

After that you can crawl for updates whenever you like.


### Usage

```bash
>>> python blog_notifier.py --help
>>> python blog_notifier.py -migrate  # create sqlite3 database with empty tables
>>> python blog_notifier.py -crawl    # crawl new articles and send email to client
>>> python blog_notifier.py -explore  https://sysadmin.pm/  # add site to watchlist
```

### Example configuration

```yaml
mode: mail

server:
  host: smtp.gmail.com
  port: 465

client:
  email: example@gmail.com
  password: examplepassword
  send_to: example@gmail.com

telegram:
  bot_token: token
  channel: @channel
```

Also you can define client's credentiials with environment variables.

```bash
$ export NOTIFIER_CONFIG='credentials.yml'
$ export NOTIFIER_DB='db.sqlite3'
$ export NOTIFIER_CLIENT_EMAIL='example@gmail.com'
$ export NOTIFIER_CLIENT_PASSWORD='examplepassword'
$ export NOTIFIER_CLIENT_SEND_TO='example@gmail.com'
$ export NOTIFIER_TELEGRAM_BOT_TOKEN='token'
$ export NOTIFIER_TELEGRAM_CHANNEL='@channel'
```
