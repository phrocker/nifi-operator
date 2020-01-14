package nifi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var logger = logf.Log.WithName("controller_nifi").WithValues("NiFi client")

//ClusterDescriptor represents a cluster of NiFi nodes
type ClusterDescriptor struct {
	Cluster Cluster `json:"cluster"`
}

//Cluster represents a cluster of NiFi nodes
type Cluster struct {
	Nodes []Node `json:"nodes"`
}

//Node Represents a node in a NIFI cluster
type Node struct {
	NodeID  string `json:"nodeId"`
	Address string `json:"address"`
	Status  string `json:"status"`
}

//Client is a client for comunicating with the NiFi API
type Client struct {
	baseURL string
}

// NewClient Return a new NiFI Client
func NewClient(baseURL string) Client {
	return Client{baseURL: baseURL}
}

// Healthy Returns if the service is responding
func (c Client) Healthy() bool {
	serverURL := "http://" + c.baseURL + ":8080/nifi-api/controller/cluster/"

	_, err := http.Get(serverURL)
	if err != nil {
		return false
	}

	return true
}

//GetCluster Returns a cluster descriptor
func (c Client) GetCluster() (Cluster, error) {
	serverURL := "http://" + c.baseURL + ":8080/nifi-api/controller/cluster/"
	logger.Info("Getting node list from NiFi", "NiFi server Address", serverURL)
	resp, err := http.Get(serverURL)
	if err != nil {
		return Cluster{}, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Cluster{}, err
	}

	var clusterDesc ClusterDescriptor
	json.Unmarshal(body, &clusterDesc)
	cluster := clusterDesc.Cluster

	return cluster, nil
}

//DeleteMissing Remove nodes that are not in the podlist
func (c Client) DeleteMissing(podList []string) error {

	cluster, err := c.GetCluster()
	if err != nil {
		return err
	}

	for _, node := range cluster.Nodes {
		nodeName := getNodeName(node)
		if findPod(nodeName, podList) == false {
			c.removeNode(node)
		}
	}

	return nil
}

func (c Client) removeNode(n Node) error {
	httpClient := &http.Client{}
	requestURL := "http://" + c.baseURL + ":8080/nifi-api/controller/cluster/nodes/" + n.NodeID

	logger.Info("Deleting Node", "NodeID", n.NodeID, "Node Address", n.Address, "Node Status", n.Status)

	if n.Status == "CONNECTED" {
		logger.Info("Found missing pod with status Connected, not deleting...")
		return fmt.Errorf("found missing pod with status CONNECTED, not deleting")
	}

	req, err := http.NewRequest("DELETE", requestURL, nil)
	if err != nil {
		logger.Info(err.Error())
		return err
	}

	// Fetch Request
	resp, err := httpClient.Do(req)
	if err != nil {
		logger.Info(err.Error())
		return nil
	}
	defer resp.Body.Close()

	// Read Response Body
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Info(err.Error())
		return nil
	}

	return nil
}

func (c Client) findDisconnected() ([]string, error) {
	cluster, err := c.GetCluster()
	if err != nil {
		return nil, err
	}

	nodeList := []string{}
	for _, node := range cluster.Nodes {
		if node.Status == "DISCONNECTED" {
			nodeList = append(nodeList, getNodeName(node))
		}
	}

	return nodeList, nil
}

func findPod(n string, podList []string) bool {
	for _, p := range podList {
		if n == p {
			return true
		}
	}
	return false
}

func getNodeName(node Node) string {
	return strings.Split(node.Address, ".")[0]
}
