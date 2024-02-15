
DOCKER COMMANDS:
----------------

1. pull the docker
  ```bash
    docker pull mcr.microsoft.com/mssql/server:2022-latest
```

2. run the docker container (update the creds)
```bash
    docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=DMF@2023" -p 1433:1433 --name DMFDB --hostname DMFDB -d mcr.microsoft.com/mssql/server:2022-latest
```
3. see logs
  ```bash
    sudo docker exec -t DMFDB cat /var/opt/mssql/log/errorlog | grep connection
  ```

Connect to SQL Server:
----------------------

1. ``` docker exec -it DMFDB "bash"  ```
2. ```/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "DMF@2023"```

Official URL: [Microsoft SQL Server Docker Quickstart Guide](https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-ver16&pivots=cs1-cmd)
