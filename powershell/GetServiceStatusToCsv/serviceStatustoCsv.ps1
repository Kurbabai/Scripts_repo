gwmi win32_service -comp (gc .\servers.txt) -filter "state='running' and name like '%dhcp%'" | select __server,name,startmode,state,status | export-csv .\servers.csv 