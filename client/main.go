package main

import (
	"flag"
	"fmt"
	"net/http"
)

func main() {

	var servers = []string{"http://localhost:8000", "http://localhost:8001"}

	clientID := flag.Int("id", 0, "Client ID")
	flag.Parse()

	for {
		fmt.Printf("Client %d: Enter bid amount: ", *clientID)
		var amount int
		_, err := fmt.Scanf("%d", &amount)
		if err != nil {
			fmt.Println("Invalid input. Please enter a valid integer.")
			continue
		}

		bidAmount(*clientID, amount, servers)
	}

}

func bidAmount(clientID int, amount int, servers []string) {
	// Try to first send the bid to the first server in the list if its dead then send it to the next server in the list and so on
	for _, server := range servers {
		url := fmt.Sprintf("%s/bid?amount=%d", server, amount)
		req, err := http.NewRequest("PUT", url, nil)
		if err != nil {
			fmt.Printf("Client %d: Failed to create request: %v\n", clientID, err)
			continue
		}
		req.Header.Set("X-Client-ID", fmt.Sprintf("%d", clientID))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("Client %d: Failed to send bid to %s: %v\n", clientID, server, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			fmt.Printf("Client %d: Successfully placed bid of %d to %s\n", clientID, amount, server)
			return
		} else {
			fmt.Printf("Client %d: Failed to place bid to %s: %s\n", clientID, server, resp.Status)
		}
	}

	fmt.Printf("Client %d: Failed to place bid of %d to all servers\n", clientID, amount)
}
