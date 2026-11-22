package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
)

func main() {

	var servers = []string{"http://localhost:8000", "http://localhost:8001", "http://localhost:8002"}

	clientID := flag.Int("id", 0, "Client ID")
	flag.Parse()
	fmt.Printf("Commands: <amount> to bid, result to query, exit to quit\n")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("Client %d: Enter bid amount: ", *clientID)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				fmt.Printf("Failed to read input: %v\n", err)
			}
			return
		}

		if scanner.Text() == "exit" {
			fmt.Printf("Client %d: Exiting...\n", *clientID)
			return
		}

		if scanner.Text() == "result" {
			printResult(*clientID, servers)
			continue
		}

		amount, err := strconv.Atoi(scanner.Text())
		if err != nil || amount <= 0 {
			fmt.Println("Invalid input. Please enter a positive integer.")
			continue
		}

		bidAmount(*clientID, amount, servers)
	}

}

func printResult(clientID int, servers []string) {
	httpClient := &http.Client{}
	for _, server := range servers {
		resp, err := httpClient.Get(server + "/result")
		if err != nil {
			fmt.Printf("Client %d: Failed to create request %v\n", clientID, err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("Client %d: Failed to read response from %s: %v\n", clientID, server, err)
		}

		if resp.StatusCode == http.StatusOK {
			fmt.Printf("Client %d: Result from %s: %s", clientID, server, body)
			return
		}
	}
	fmt.Printf("Client %d: Failed to get result from all servers\n", clientID)
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
