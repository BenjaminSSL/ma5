package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	heartbeatInterval = 2 * time.Second
	heartbeatTimeout  = 10 * time.Second
	electionTimeout   = 5 * time.Second
	requestTimeout    = 2 * time.Second
)

type Bid struct {
	ClientID int `json:"client_id"`
	Amount   int `json:"amount"`
}

type Server struct {
	otherNodes map[int]string
	id         int
	server     *http.ServeMux

	mu                 sync.Mutex
	leader             bool
	leaderID           int
	electionInProgress bool
	electionStarted    time.Time
	lastHeartbeat      time.Time

	bidMu sync.Mutex
	bids  map[int]int
	log   []Bid
}

func main() {
	id := flag.Int("id", 0, "Server ID")
	flag.Parse()

	if *id < 0 || *id > 2 {
		log.Fatal("Server ID must be 0, 1 or 2")
	}

	nodes := map[int]string{
		0: "http://localhost:8000",
		1: "http://localhost:8001",
		2: "http://localhost:8002",
	}
	delete(nodes, *id)

	s := &Server{
		otherNodes:    nodes,
		id:            *id,
		leader:        *id == 0,
		leaderID:      0,
		bids:          make(map[int]int),
		lastHeartbeat: time.Now(),
	}
	s.initServer()

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", 8000+*id),
		Handler:           s.server,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go s.heartbeatLoop()
	go s.syncLoop()

	go func() {
		log.Printf("Server %d listening on :%d", *id, 8000+*id)
		if err := httpServer.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
}

func (s *Server) initServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /bid", s.bid)
	mux.HandleFunc("GET /result", s.result)
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /heartbeat", s.heartbeat)
	mux.HandleFunc("POST /election", s.election)
	mux.HandleFunc("POST /coordinator", s.coordinator)
	mux.HandleFunc("GET /sync", s.syncBids)
	s.server = mux
}

func client() *http.Client {
	return &http.Client{Timeout: requestTimeout}
}

func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		isLeader := s.leader
		last := s.lastHeartbeat
		inElection := s.electionInProgress
		started := s.electionStarted
		leaderID := s.leaderID
		s.mu.Unlock()

		if isLeader {
			for id, url := range s.otherNodes {
				go func(id int, url string) {
					endpoint := fmt.Sprintf(
						"%s/heartbeat?id=%d", url, s.id,
					)
					resp, err := client().Post(endpoint, "", nil)
					if err != nil {
						log.Printf("Server %d: heartbeat to %d failed: %v",
							s.id, id, err)
						return
					}
					resp.Body.Close()
				}(id, url)
			}
			continue
		}

		if inElection {
			if time.Since(started) > electionTimeout {
				s.mu.Lock()
				if s.electionInProgress &&
					s.electionStarted.Equal(started) {
					s.electionInProgress = false
				}
				s.mu.Unlock()
				go s.startElection()
			}
			continue
		}

		if time.Since(last) > heartbeatTimeout {
			log.Printf("Server %d: leader %d timed out",
				s.id, leaderID)
			go s.startElection()
		}
	}
}

func (s *Server) startElection() {
	s.mu.Lock()
	if s.electionInProgress {
		s.mu.Unlock()
		return
	}
	s.electionInProgress = true
	s.electionStarted = time.Now()
	s.leader = false
	s.mu.Unlock()

	log.Printf("Server %d: starting election", s.id)

	higherResponded := false

	for id, url := range s.otherNodes {
		if id <= s.id {
			continue
		}

		resp, err := client().Post(url+"/election", "", nil)
		if err != nil {
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			higherResponded = true
		}
	}

	if higherResponded {
		log.Printf("Server %d: waiting for higher node", s.id)
		return
	}

	s.becomeLeader()
}

func (s *Server) election(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
	go s.startElection()
}

func (s *Server) becomeLeader() {
	s.mu.Lock()
	s.leader = true
	s.leaderID = s.id
	s.electionInProgress = false
	s.lastHeartbeat = time.Now()
	s.mu.Unlock()

	log.Printf("Server %d: became leader", s.id)

	for _, url := range s.otherNodes {
		go func(url string) {
			endpoint := fmt.Sprintf(
				"%s/coordinator?id=%d", url, s.id,
			)
			resp, err := client().Post(endpoint, "", nil)
			if err != nil {
				log.Printf("Coordinator notification failed: %v", err)
				return
			}
			resp.Body.Close()
		}(url)
	}
}

func (s *Server) coordinator(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.URL.Query().Get("id"))
	if err != nil || id < 0 || id > 2 {
		http.Error(w, "Invalid leader ID", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if s.leader && s.id > id {
		s.mu.Unlock()
		http.Error(w, "Lower-priority coordinator", http.StatusConflict)
		return
	}
	s.leaderID = id
	s.leader = s.id == id
	s.electionInProgress = false
	s.lastHeartbeat = time.Now()
	s.mu.Unlock()

	log.Printf("Server %d: new leader is %d", s.id, id)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.URL.Query().Get("id"))
	if err != nil {
		http.Error(w, "Invalid leader ID", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if id != s.leaderID || s.leader {
		s.mu.Unlock()
		http.Error(w, "Unexpected leader", http.StatusConflict)
		return
	}
	s.lastHeartbeat = time.Now()
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	leaderID := s.leaderID
	isLeader := s.leader
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"id":        s.id,
		"leader":    isLeader,
		"leader_id": leaderID,
	})
}

func maxBid(bids map[int]int) int {
	max := 0
	for _, amount := range bids {
		if amount > max {
			max = amount
		}
	}
	return max
}

func (s *Server) bid(w http.ResponseWriter, r *http.Request) {
	clientID, err := strconv.Atoi(r.Header.Get("X-Client-ID"))
	if err != nil {
		http.Error(w, "Invalid client ID", http.StatusBadRequest)
		return
	}

	amount, err := strconv.Atoi(r.URL.Query().Get("amount"))
	if err != nil || amount <= 0 {
		http.Error(w, "Invalid amount", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	isLeader := s.leader
	leaderID := s.leaderID
	s.mu.Unlock()

	// Only the leader accepts external bids.
	if !isLeader {
		leaderURL := fmt.Sprintf(
			"http://localhost:%d",
			8000+leaderID,
		)

		proxyClient := &http.Client{
			Timeout: 5 * time.Second,
		}

		req, err := http.NewRequestWithContext(
			r.Context(),
			r.Method,
			leaderURL+r.URL.RequestURI(),
			r.Body,
		)
		if err != nil {
			http.Error(w, "Failed to forward bid", http.StatusInternalServerError)
			return
		}

		req.Header = r.Header.Clone()

		resp, err := proxyClient.Do(req)
		if err != nil {
			http.Error(w, "Leader unavailable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		// Forward the leader's response to the client.
		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}

		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	s.bidMu.Lock()
	defer s.bidMu.Unlock()

	s.mu.Lock()
	if !s.leader {
		s.mu.Unlock()
		http.Error(w, "Leadership changed", http.StatusConflict)
		return
	}

	if amount <= maxBid(s.bids) {
		s.mu.Unlock()
		http.Error(w, "Bid must exceed current highest bid",
			http.StatusBadRequest)
		return
	}

	b := Bid{ClientID: clientID, Amount: amount}
	s.bids[clientID] = amount
	s.log = append(s.log, b)
	s.mu.Unlock()

	log.Printf("Server %d: accepted bid %+v", s.id, b)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(b)
}

func (s *Server) syncBids(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	data := append([]Bid(nil), s.log...)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) syncLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		isLeader := s.leader
		leaderID := s.leaderID
		s.mu.Unlock()

		if isLeader || leaderID == s.id {
			continue
		}

		url := fmt.Sprintf(
			"http://localhost:%d/sync", 8000+leaderID,
		)
		resp, err := client().Get(url)
		if err != nil {
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}

		var history []Bid
		err = json.NewDecoder(resp.Body).Decode(&history)
		resp.Body.Close()
		if err != nil {
			continue
		}

		s.mu.Lock()
		if !s.leader && s.leaderID == leaderID {
			s.log = history
			s.bids = make(map[int]int)
			for _, b := range history {
				s.bids[b.ClientID] = b.Amount
			}
		}
		s.mu.Unlock()
	}
}

func (s *Server) result(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	highest := maxBid(s.bids)
	var winner int
	for id, amount := range s.bids {
		if amount == highest {
			winner = id
			break
		}
	}
	count := len(s.log)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{
		"winner":      winner,
		"highest_bid": highest,
		"bid_count":   count,
	})
}
