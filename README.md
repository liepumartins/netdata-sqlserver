# netdata-sqlserver
netdata python plugin for sqlserver statistics

SQL Server 2017 is available on linux and [netdata](https://github.com/firehol/netdata) is best monitoring tool, therefore this plugin.

Everything is work in progress.

Currently collects performance counter data from `sys.dm_os_performance_counters`

## TODO
* define all charts
* organize charts to provide most meaningful results
* implement lock/wait type data collection
* screenshots
* documentation

## Credits
Based on netdata postgres plugin by: [facetoe](https://github.com/facetoe)

[dangtranhoang](https://github.com/dangtranhoang)

Uses [pymssql](http://pymssql.org/) library