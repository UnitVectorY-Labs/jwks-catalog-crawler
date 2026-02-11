package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime/debug"

	"cloud.google.com/go/pubsub"
	"gopkg.in/yaml.v2"
)

// Version is the application version, injected at build time via ldflags
var Version = "dev"

// Service represents a JWKS service
type Service struct {
	Id                  string `yaml:"id"`
	Name                string `yaml:"name"`
	OpenIDConfiguration string `yaml:"openid-configuration"`
	JWKSURI             string `yaml:"jwks_uri"`
}

// Data holds the list of services and content
type Data struct {
	Services []Service `yaml:"services"`
}

// CrawlRequest represents the payload sent to Pub/Sub
type CrawlRequest struct {
	URL string `json:"url"`
}

func main() {
	// Set the build version from the build info if not set by the build system
	if Version == "dev" || Version == "" {
		if bi, ok := debug.ReadBuildInfo(); ok {
			if bi.Main.Version != "" && bi.Main.Version != "(devel)" {
				Version = bi.Main.Version
			}
		}
	}

	// Log the version
	log.Printf("JWKS Catalog Crawler version: %s", Version)

	// Load environment variables
	yamlURL := os.Getenv("YAML_CATALOG_URL")
	projectID := os.Getenv("GCP_PROJECT_ID")
	topicName := os.Getenv("PUBSUB_TOPIC_NAME")

	if yamlURL == "" || projectID == "" || topicName == "" {
		log.Fatal("Missing required environment variables: YAML_CATALOG_URL, GCP_PROJECT_ID, PUBSUB_TOPIC_NAME")
	}

	// Load the YAML file in and parse it as the Data struct
	data, err := fetchAndParseYAML(yamlURL)
	if err != nil {
		log.Fatalf("Failed to load YAML file: %v", err)
	}

	// Make an array of all of the URLs both OIDC and JWKS
	var urls []string
	for _, service := range data.Services {
		if service.OpenIDConfiguration != "" {
			urls = append(urls, service.OpenIDConfiguration)
		}
		if service.JWKSURI != "" {
			urls = append(urls, service.JWKSURI)
		}
	}

	// Publish the URLs to Pub/Sub
	if err := publishCrawlRequests(projectID, topicName, urls); err != nil {
		log.Fatalf("Failed to publish crawl requests: %v", err)
	}
}

// fetchAndParseYAML retrieves and parses the JWKS catalog YAML file
func fetchAndParseYAML(url string) (*Data, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch YAML file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch YAML: status code %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read YAML content: %w", err)
	}

	var config Data
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	return &config, nil
}

// publishCrawlRequests sends each JWKS URL as a crawl request to Pub/Sub
func publishCrawlRequests(projectID, topicName string, urls []string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}
	defer client.Close()

	topic := client.Topic(topicName)
	defer topic.Stop()

	for _, url := range urls {
		req := CrawlRequest{
			URL: url,
		}

		msgData, err := json.Marshal(req)
		if err != nil {
			log.Printf("Skipping URL %s due to JSON marshalling error: %v", url, err)
			continue
		}

		result := topic.Publish(ctx, &pubsub.Message{
			Data: msgData,
		})

		// Wait for result to complete
		id, err := result.Get(ctx)
		if err != nil {
			log.Printf("Failed to publish message for URL %s: %v", url, err)
			continue
		}

		log.Printf("Published crawl request for %s with message ID: %s", url, id)
	}

	return nil
}
