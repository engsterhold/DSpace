# Local Commands

```
docker-compose -f docker-compose.yml -f docker-compose-cli.yml build --build-arg JDK_VERSION=17
```

```
docker-compose -p d7 -f docker-compose.yml  -f dspace/src/main/docker-compose/docker-compose-iiif.yml -f dspace/src/main/docker-compose/docker-compose-angular.yml up -d
```

```
docker-compose -p d7 -f docker-compose-cli.yml run --rm dspace-cli create-administrator -e engsterhold@uni-marburg.de -f robert -l user -p robert -c de
```

```
docker-compose -p d7 -f docker-compose-cli.yml run --rm dspace-cli [command] [parameters]
```

```
docker-compose rm -s -v my_service
```