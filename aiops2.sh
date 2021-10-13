CONN=10.0.0.78:7849

while : 
do
#curl -X POST ${CONN} -H 'command: run mqtt client where broker = rest and port=2049 and user-agent = anylog and topic = (name = "aiops" and dbms = "bring [dbms]" and table = "bring [table]" and column.timestamp.timestamp = "bring [timestamp]" and column.value.float = "bring [value]")' -H "User-Agent: AnyLog/1.23"

# POST data +00:00
curl -X POST ${CONN} -H "command: data"  -H "User-Agent: AnyLog/1.23" -H "Content-Type: text/plain" --data-raw '{"timestamp": "2021-06-18 23:08:48.114672+00:00", "value": 67.67896865405338, "tag_id": 3, "dbms": "al_smoke_test", "table": "fic11"}'

curl -X GET ${CONN} -H "command: get msg client"  -H "User-Agent: AnyLog/1.23" 
done 
