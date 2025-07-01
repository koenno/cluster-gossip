package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/memberlist"
)

// Message represents a custom message structure
type Message struct {
	Type    string      `json:"type"`
	From    string      `json:"from"`
	Content interface{} `json:"content"`
	Time    time.Time   `json:"time"`
}

// ClusterNode represents a node in the cluster
type ClusterNode struct {
	name       string
	memberlist *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
}

// Delegate implements memberlist.Delegate interface
type Delegate struct {
	node *ClusterNode
}

// NodeMeta implements memberlist.Delegate
func (d *Delegate) NodeMeta(limit int) []byte {
	meta := map[string]string{
		"role":    "worker",
		"version": "1.0.0",
	}
	data, _ := json.Marshal(meta)
	return data
}

// NotifyMsg implements memberlist.Delegate - handles received messages
func (d *Delegate) NotifyMsg(data []byte) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return
	}

	fmt.Printf("Received message: Type=%s, From=%s, Content=%v, Time=%s\n",
		msg.Type, msg.From, msg.Content, msg.Time.Format(time.RFC3339))
}

// GetBroadcasts implements memberlist.Delegate
func (d *Delegate) GetBroadcasts(overhead, limit int) [][]byte {
	//fmt.Println("GetBroadcasts", overhead, limit)
	return d.node.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState implements memberlist.Delegate
func (d *Delegate) LocalState(join bool) []byte {
	state := map[string]interface{}{
		"status": "active",
		"join":   join,
	}
	data, _ := json.Marshal(state)
	return data
}

// MergeRemoteState implements memberlist.Delegate
func (d *Delegate) MergeRemoteState(buf []byte, join bool) {
	var state map[string]interface{}
	if err := json.Unmarshal(buf, &state); err == nil {
		fmt.Printf("Merged remote state: %v (join=%t)\n", state, join)
	}
}

// EventDelegate implements memberlist.EventDelegate interface
type EventDelegate struct {
	node *ClusterNode
}

// NotifyJoin implements memberlist.EventDelegate
func (e *EventDelegate) NotifyJoin(node *memberlist.Node) {
	fmt.Printf("Node joined: %s (%s)\n", node.Name, node.Addr)
}

// NotifyLeave implements memberlist.EventDelegate
func (e *EventDelegate) NotifyLeave(node *memberlist.Node) {
	fmt.Printf("Node left: %s (%s)\n", node.Name, node.Addr)
}

// NotifyUpdate implements memberlist.EventDelegate
func (e *EventDelegate) NotifyUpdate(node *memberlist.Node) {
	fmt.Printf("Node updated: %s (%s)\n", node.Name, node.Addr)
}

// NewClusterNode creates a new cluster node
func NewClusterNode(name string, bindPort int) (*ClusterNode, error) {
	node := &ClusterNode{
		name: name,
	}

	// Create configuration
	config := memberlist.DefaultLocalConfig()
	config.Name = name
	config.BindPort = bindPort
	config.AdvertisePort = bindPort

	// Set delegates
	config.Delegate = &Delegate{node: node}
	config.Events = &EventDelegate{node: node}

	// Create memberlist
	ml, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %v", err)
	}

	node.memberlist = ml

	// Initialize broadcast queue
	node.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return ml.NumMembers()
		},
		RetransmitMult: 3, // Retransmit 3 times
	}

	return node, nil
}

// Join joins an existing cluster
func (n *ClusterNode) Join(addresses []string) error {
	_, err := n.memberlist.Join(addresses)
	return err
}

// Broadcast sends a message to all nodes in the cluster
func (n *ClusterNode) Broadcast(msgType string, content interface{}) error {
	msg := Message{
		Type:    msgType,
		From:    n.name,
		Content: content,
		Time:    time.Now(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	// Queue the broadcast
	n.broadcasts.QueueBroadcast(&broadcast{
		msg:    data,
		notify: nil,
	})

	return nil
}

// SendToNode sends a message directly to a specific node
func (n *ClusterNode) SendToNode(nodeName string, msgType string, content interface{}) error {
	msg := Message{
		Type:    msgType,
		From:    n.name,
		Content: content,
		Time:    time.Now(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	// Find the target node
	for _, member := range n.memberlist.Members() {
		if member.Name == nodeName {
			return n.memberlist.SendReliable(member, data)
		}
	}

	return fmt.Errorf("node %s not found", nodeName)
}

// GetMembers returns all cluster members
func (n *ClusterNode) GetMembers() []*memberlist.Node {
	return n.memberlist.Members()
}

// Leave gracefully leaves the cluster
func (n *ClusterNode) Leave() error {
	return n.memberlist.Leave(time.Second * 5)
}

// Shutdown shuts down the node
func (n *ClusterNode) Shutdown() error {
	return n.memberlist.Shutdown()
}

// broadcast implements memberlist.Broadcast interface
type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

// Example usage
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <node-name> [join-address]")
		os.Exit(1)
	}

	nodeName := os.Args[1]
	nodePort, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("failed to convert port '%s' to integer", os.Args[2])
	}

	// Create node
	node, err := NewClusterNode(nodeName, nodePort)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Shutdown()

	// Join existing cluster if address provided
	if len(os.Args) > 3 {
		joinAddr := os.Args[3]
		if err := node.Join([]string{joinAddr}); err != nil {
			log.Printf("Failed to join cluster: %v", err)
		}
	}

	fmt.Printf("Node %s started on port 7946\n", nodeName)
	fmt.Printf("Current members: %d\n", len(node.GetMembers()))

	//// Example: Send periodic broadcasts
	//go func() {
	//	ticker := time.NewTicker(10 * time.Second)
	//	defer ticker.Stop()
	//
	//	counter := 0
	//	for range ticker.C {
	//		counter++
	//		err := node.Broadcast("heartbeat", map[string]interface{}{
	//			"counter": counter,
	//			"status":  "alive",
	//		})
	//		if err != nil {
	//			log.Printf("Failed to broadcast: %v", err)
	//		}
	//	}
	//}()
	//
	//// Example: Send direct message to first member (if any)
	//go func() {
	//	time.Sleep(5 * time.Second)
	//	members := node.GetMembers()
	//	if len(members) > 1 {
	//		for _, member := range members {
	//			if member.Name != nodeName {
	//				err := node.SendToNode(member.Name, "direct", "Hello from "+nodeName)
	//				if err != nil {
	//					log.Printf("Failed to send direct message: %v", err)
	//				}
	//				break
	//			}
	//		}
	//	}
	//}()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			var nodes []string
			for _, member := range node.memberlist.Members() {
				nodes = append(nodes, member.Name)
			}
			log.Printf("cluster status: %v", nodes)
		}
	}()

	// Keep running
	select {}
}
