BACKUP_NAME=blogs.sqlite3.$(shell date +'%Y-%m-%dT%H-%M-%S').zip
BACKUP_FILE=/tmp/${BACKUP_NAME}
BLOGS_DB=blogs.sqlite3

.PHONY: backup
backup:
	@zip ${BACKUP_FILE} ${BLOGS_DB} &> /dev/null
	@echo ${BACKUP_FILE}
