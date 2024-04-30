package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v3"
)

// Config structure for storing RabbitMQ configuration from YAML file
type Config struct {
	RabbitMQ struct {
		Host           string `yaml:"host"`
		Port           int    `yaml:"port"`
		VirtualHost    string `yaml:"virtual_host"`
		Username       string `yaml:"username"`
		PasswordBase64 string `yaml:"password_base64"` // Password in base64 format
		TestQueue      string `yaml:"test_queue"`
		Exchange       string `yaml:"exchange"`
		Message        string `yaml:"message"`
	} `yaml:"rabbitmq"`
}

var httpClient = &http.Client{} // Shared HTTP client with connection pooling
var mutex sync.Mutex            // Mutex for ensuring thread safety

// loadConfig function to load configuration from YAML file
func loadConfig(filePath string) (*Config, error) {
	config := &Config{}

	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(yamlFile, config)
	if err != nil {
		return nil, err
	}

	// Decode password from base64
	password, err := base64.StdEncoding.DecodeString(config.RabbitMQ.PasswordBase64)
	if err != nil {
		return nil, err
	}
	config.RabbitMQ.PasswordBase64 = string(password)

	return config, nil
}

// createVirtualHost function to create VirtualHost in RabbitMQ if it doesn't exist
func createVirtualHost(config *Config) error {
	mutex.Lock()
	defer mutex.Unlock()

	url := fmt.Sprintf("http://%s:%d/api/vhosts/%s", config.RabbitMQ.Host, 15672, config.RabbitMQ.VirtualHost)
	username := config.RabbitMQ.Username
	password := config.RabbitMQ.PasswordBase64
	requestBody := []byte(fmt.Sprintf(`{"type":"direct","auto_delete":false,"durable":true,"internal":false,"node":"rabbit@%s"}`, config.RabbitMQ.Host))

	// Send GET request to check the existence of VirtualHost
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// If VirtualHost already exists, exit without error
	if resp.StatusCode == http.StatusOK {
		fmt.Printf("VirtualHost %s already exists.\n", config.RabbitMQ.VirtualHost)
		return nil
	}

	// If VirtualHost doesn't exist, create it
	req, err = http.NewRequest("PUT", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err = httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusPreconditionFailed {
		return fmt.Errorf("failed to create VirtualHost, status: %s", resp.Status)
	}

	fmt.Printf("VirtualHost %s created or already exists.\n", config.RabbitMQ.VirtualHost)
	return nil
}

// createExchange function to create Exchange in RabbitMQ if it doesn't exist
func createExchange(config *Config) error {
	conn, ch, err := connectToRabbitMQ(config)
	if err != nil {
		return err
	}
	defer disconnectFromRabbitMQ(conn, ch)

	err = ch.ExchangeDeclare(
		config.RabbitMQ.Exchange, // name
		"direct",                 // kind
		true,                     // durable
		false,                    // autoDelete
		false,                    // internal
		false,                    // noWait
		nil,                      // args
	)
	if err != nil {
		return err
	}

	fmt.Printf("Exchange %s created or already exists.\n", config.RabbitMQ.Exchange)
	return nil
}

func connectToRabbitMQ(config *Config) (*amqp091.Connection, *amqp091.Channel, error) {
	uri := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		url.QueryEscape(config.RabbitMQ.Username),
		url.QueryEscape(config.RabbitMQ.PasswordBase64),
		config.RabbitMQ.Host,
		config.RabbitMQ.Port,
		url.QueryEscape(config.RabbitMQ.VirtualHost))

	conn, err := amqp091.Dial(uri)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, ch, nil
}

func disconnectFromRabbitMQ(conn *amqp091.Connection, ch *amqp091.Channel) {
	if ch != nil {
		ch.Close()
	}
	if conn != nil {
		conn.Close()
	}
}

// testRabbitMQAccess function to check RabbitMQ access
func testRabbitMQAccess(config *Config) {
	conn, ch, err := connectToRabbitMQ(config)
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %s", err)
	}
	defer disconnectFromRabbitMQ(conn, ch)

	fmt.Printf("Success: Connected to RabbitMQ. Host: %s, Port: %d\n", config.RabbitMQ.Host, config.RabbitMQ.Port)

	_, err = ch.QueueDeclare(
		config.RabbitMQ.TestQueue, 
		true, 
		false, 
		false, 
		false, 
		nil
	)
	if err != nil {
		log.Fatalf("Error creating queue: %s", err)
	}

	fmt.Printf("Success: Queue '%s' created.\n", config.RabbitMQ.TestQueue)

	err = ch.QueueBind(
		config.RabbitMQ.TestQueue, 
		config.RabbitMQ.TestQueue, 
		config.RabbitMQ.Exchange, 
		false, 
		nil
	)
	if err != nil {
		log.Fatalf("Error creating binding: %s", err)
	}

	fmt.Printf("Success: Binding '%s' created.\n", config.RabbitMQ.TestQueue)

	messageBody := []byte(config.RabbitMQ.Message)
	err = ch.Publish(
		config.RabbitMQ.Exchange, 
		config.RabbitMQ.TestQueue, 
		false, 
		false, 
		amqp091.Publishing{ContentType: "text/plain", Body: messageBody}
	)
	if err != nil {
		log.Fatalf("Error sending message: %s", err)
	}

	fmt.Println("Success: Sent the following message:", config.RabbitMQ.Message)

	msgs, err := ch.Consume(
		config.RabbitMQ.TestQueue, 
		"", 
		false, 
		false, 
		false, 
		false, 
		nil
	)
	if err != nil {
		log.Fatalf("Error receiving message: %s", err)
	}

	fmt.Println("\nReceiving message...\n")

	for msg := range msgs {
		if string(msg.Body) == string(messageBody) {
			fmt.Printf("Success: Received expected message: %s\n", msg.Body)
			// Acknowledge receiving the message
			msg.Ack(false)
			break
		} else {
			fmt.Printf("Error: Received unexpected message: %s\n", msg.Body)
			break
		}
	}
}

func main() {
	args := os.Args
	if len(args) < 3 {
		log.Fatal("Usage: rqat --config <config_file_path> [--admin]")
	}

	var configFilePath string
	var isAdmin bool

	for i := 1; i < len(args); i++ {
		if args[i] == "--config" && i+1 < len(args) {
			configFilePath = args[i+1]
			i++ // skip the next argument since it's the config file path
		} else if args[i] == "--admin" {
			isAdmin = true
		}
	}

	if configFilePath == "" {
		log.Fatal("Configuration file path is missing")
	}

	config, err := loadConfig(configFilePath)
	if err != nil {
		log.Fatalf("Error loading configuration: %s", err)
	}

	if isAdmin {
		if err := createVirtualHost(config); err != nil {
			log.Fatalf("Error creating VirtualHost: %s", err)
		}
		if err := createExchange(config); err != nil {
			log.Fatalf("Error creating Exchange: %s", err)
		}
		fmt.Println("Check exist VirtualHost and Exchange completed successfully.")
	}

	testRabbitMQAccess(config)
}
