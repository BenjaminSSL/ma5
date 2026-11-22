### MA5 – Distributed Auction

Start the three servers (separate terminals), then a client:

```
go run ./server -id 0
go run ./server -id 1
go run ./server -id 2
go run ./client -id 1
```

Client commands: `<amount>` to bid, `result` to query, `exit` to quit.

The auction lasts 67 seconds from server start; restart the servers for a new one.
When the auction ends and the clinet attempts to place a bid it will just get refused, so this is the indicator...
Client can ALWAYS use `result` though.
