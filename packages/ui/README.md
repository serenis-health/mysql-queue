# UI

Docker build

```bash
docker build -f packages/ui/Dockerfile -t mysql-queue-ui .
docker run -p 3000:3000 -e DB_URI="mysql://user:password@localhost:3306/dbname" mysql-queue-ui
```
