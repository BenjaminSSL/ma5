package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"slices"
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
	auctionTime       = 67 * time.Second
)

type Bid struct {
	ClientID int `json:"client_id"`
	Amount   int `json:"amount"`
}

type AuctionState struct {
	Log     []Bid     `json:"log"`
	EndTime time.Time `json:"end_time"`
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
	auctionEndTime     time.Time

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
		otherNodes:     nodes,
		id:             *id,
		leader:         *id == 0,
		leaderID:       0,
		bids:           make(map[int]int),
		lastHeartbeat:  time.Now(),
		auctionEndTime: time.Now().Add(auctionTime),
	}
	s.initServer()

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", 8000+*id),
		Handler:           s.server,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go s.heartbeatLoop()

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
	mux.HandleFunc("GET /replica", s.readReplica)
	mux.HandleFunc("PUT /replica", s.storeReplica)
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
		endTime := s.auctionEndTime
		s.mu.Unlock()

		if isLeader {
			for id, url := range s.otherNodes {
				go func(id int, url string) {
					endpoint := fmt.Sprintf(
						"%s/heartbeat?id=%d&end=%d", url, s.id, endTime.UnixNano(),
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
	if s.leader || s.electionInProgress || time.Since(s.lastHeartbeat) <= heartbeatTimeout {
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
    oldLeader := s.leaderID
    s.mu.Unlock()

    acknowledgements := 1 // Count ourselves
    latestState := AuctionState{}

    s.mu.Lock()
    latestState.Log = slices.Clone(s.log)
    latestState.EndTime = s.auctionEndTime
    s.mu.Unlock()

    for id, url := range s.otherNodes {
        if id == oldLeader {
            continue
        }

        state, err := fetchReplica(url)
        if err != nil {
            log.Printf("Server %d: cannot recover from node %d: %v",
                s.id, id, err)
            continue
        }

        acknowledgements++

        if len(state.Log) > len(latestState.Log) {
            latestState = state
        }
    }

    if acknowledgements < 2 {
        log.Printf("Server %d: insufficient nodes for election", s.id)
        return
    }

    s.mu.Lock()
    if !s.electionInProgress || s.leaderID != oldLeader {
        s.mu.Unlock()
        return
    }

    s.installReplica(latestState)
    s.leader = true
    s.leaderID = s.id
    s.electionInProgress = false
    s.lastHeartbeat = time.Now()
    s.mu.Unlock()

    log.Printf("Server %d: became leader", s.id)

    for _, url := range s.otherNodes {
        endpoint := fmt.Sprintf("%s/coordinator?id=%d", url, s.id)
        resp, err := client().Post(endpoint, "", nil)
        if err != nil {
            continue
        }
        resp.Body.Close()
    }
}

func fetchReplica(url string) (AuctionState, error) {
	var state AuctionState
	resp, err := client().Get(url + "/replica")
	if err != nil {
		return state, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return state, fmt.Errorf("replica returned %s", resp.Status)
	}
	err = json.NewDecoder(resp.Body).Decode(&state)
	return state, err
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
	if end, err := strconv.ParseInt(r.URL.Query().Get("end"), 10, 64); err == nil {
		s.auctionEndTime = time.Unix(0, end)
	}
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
	if !isLeader {
		forwardToLeader(w, r, leaderID)
		return
	}

	s.bidMu.Lock()
	defer s.bidMu.Unlock()

	s.mu.Lock()
	if !s.leader {
		s.mu.Unlock()
		http.Error(w, "Leadership changed", http.StatusServiceUnavailable)
		return
	}
	if !time.Now().Before(s.auctionEndTime) {
		s.mu.Unlock()
		http.Error(w, "Auction has ended", http.StatusConflict)
		return
	}
	if amount <= maxBid(s.bids) {
		s.mu.Unlock()
		http.Error(w, "Bid must exceed current highest bid", http.StatusBadRequest)
		return
	}

	b := Bid{ClientID: clientID, Amount: amount}
	state := AuctionState{
		Log:     append(slices.Clone(s.log), b),
		EndTime: s.auctionEndTime,
	}
	s.mu.Unlock()

	if !s.replicate(state) {
		http.Error(w, "Could not confirm bid replication", http.StatusServiceUnavailable)
		return
	}

	s.mu.Lock()
	if !s.leader {
		s.mu.Unlock()
		http.Error(w, "Leadership changed", http.StatusServiceUnavailable)
		return
	}
	s.bids[clientID] = amount
	s.log = state.Log
	s.mu.Unlock()

	log.Printf("Server %d: accepted bid %+v", s.id, b)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(b)
}

func forwardToLeader(w http.ResponseWriter, r *http.Request, leaderID int) {
	if r.Header.Get("X-Auction-Forwarded") != "" {
		http.Error(w, "Leader unavailable", http.StatusServiceUnavailable)
		return
	}
	url := fmt.Sprintf("http://localhost:%d%s", 8000+leaderID, r.URL.RequestURI())
	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, r.Body)
	if err != nil {
		http.Error(w, "Failed to forward request", http.StatusInternalServerError)
		return
	}
	req.Header = r.Header.Clone()
	req.Header.Set("X-Auction-Forwarded", "1")

	proxyClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := proxyClient.Do(req)
	if err != nil {
		http.Error(w, "Leader unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) replicate(state AuctionState) bool {
	body, err := json.Marshal(state)
	if err != nil {
		return false
	}

	acknowledged := make(chan bool, len(s.otherNodes))
	for _, url := range s.otherNodes {
		go func(url string) {
			endpoint := fmt.Sprintf("%s/replica?id=%d", url, s.id)
			req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(body))
			if err != nil {
				acknowledged <- false
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := client().Do(req)
			if err != nil {
				acknowledged <- false
				return
			}
			resp.Body.Close()
			acknowledged <- resp.StatusCode == http.StatusOK
		}(url)
	}

	// Attempt both followers in parallel. One acknowledgment plus the
	// leader is a majority, and promotion reads both surviving logs.
	replicated := false
	for range s.otherNodes {
		if <-acknowledged {
			replicated = true
		}
	}
	return replicated
}

func (s *Server) readReplica(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	state := AuctionState{Log: s.log, EndTime: s.auctionEndTime}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(state)
}

func (s *Server) storeReplica(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.URL.Query().Get("id"))
	var state AuctionState
	if err != nil || json.NewDecoder(r.Body).Decode(&state) != nil {
		http.Error(w, "Invalid replica update", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leader || s.electionInProgress || id != s.leaderID {
		http.Error(w, "Unexpected leader", http.StatusConflict)
		return
	}

	prefix := min(len(s.log), len(state.Log))
	if !slices.Equal(s.log[:prefix], state.Log[:prefix]) {
		http.Error(w, "Conflicting replica history", http.StatusConflict)
		return
	}
	if len(state.Log) >= len(s.log) {
		s.installReplica(state)
	}
	w.WriteHeader(http.StatusOK)
}

// installReplica requires s.mu.
func (s *Server) installReplica(state AuctionState) {
	s.log = state.Log
	s.auctionEndTime = state.EndTime
	s.bids = make(map[int]int)
	for _, b := range state.Log {
		s.bids[b.ClientID] = b.Amount
	}
}

func (s *Server) result(w http.ResponseWriter, r *http.Request) {
	s.bidMu.Lock()
	s.mu.Lock()
	if !s.leader {
		leaderID := s.leaderID
		s.mu.Unlock()
		s.bidMu.Unlock()
		forwardToLeader(w, r, leaderID)
		return
	}
	highest := maxBid(s.bids)
	result := map[string]int{"highest_bid": highest}
	if !time.Now().Before(s.auctionEndTime) {
		result["bid_count"] = len(s.log)
		for id, amount := range s.bids {
			if amount == highest {
				result["winner"] = id
				break
			}
		}
	}
	s.mu.Unlock()
	s.bidMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
