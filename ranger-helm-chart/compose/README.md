### Local Ranger + Postgres (docker-compose)

Run this from the Ranger chart directory:

```bash
cd /Users/architlatkar/Projects/dataplane-v2-helmchart/xdp/acceldata-xdp-dependencies/helmcharts/ranger
docker compose up --build
```

- **Ranger Admin UI**: `http://localhost:6080` (first start can take a few minutes while `setup.sh` runs)
- **Postgres**: `localhost:5432` (db: `ranger`, user: `root`, password: `rangerR0cks!`)

To reset everything (including DB data):

```bash
docker compose down
```

To force Postgres init scripts to run again, recreate the DB container:

```bash
docker compose up -d --force-recreate ranger-db
```


