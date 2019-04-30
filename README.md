# netdata-sqlserver
netdata python plugin for sqlserver statistics

SQL Server 2017 is available on linux and [netdata](https://github.com/firehol/netdata) is best monitoring tool, therefore this plugin.

Everything is work in progress.

Currently collects performance counter data from `sys.dm_os_performance_counters`

## Requirements

[pymssql](http://pymssql.org/) must be present on the system. Get it with something like `pip install pymssql`

SQL Server user for netdata. Granted `view server state` permission.

systemd unit file should be altered for plugin to work on boot. Append `mssql-server.service` to `After=` line.

## Installation
* Place `sqlserver.chart.py` in `/usr/libexec/netdata/python.d/`
* Place `sqlserver.conf` in `/etc/netdata/python.d/`
* Edit `/etc/netdata/python.d/sqlserver.conf`, provide username, password, database names to be monitored.
* Edit `/etc/netdata/python.d.conf`, add line `sqlserver: yes`
* Restart netdata

## Screenshots

![Transactions](/../screenshots/screenshots/transactions.png?raw=true "Example of transactions per second")

![Batch requests](/../screenshots/screenshots/batch-requests.png?raw=true "Example of batch requests per second")

## TODO
* define missing charts (suggestions are welcome)
* possibly organize charts to provide most meaningful results
* implement lock/wait type data collection
* screenshots
* documentation

## Credits
Based on netdata postgres plugin by: [facetoe](https://github.com/facetoe),[dangtranhoang](https://github.com/dangtranhoang)

Uses [pymssql](http://pymssql.org/) library
