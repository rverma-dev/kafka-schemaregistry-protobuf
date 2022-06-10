package main

import (
	"fmt"
	"github.com/rverma-nsl/kafka-schemaregistry-protobuf/lib"
	"github.com/rverma-nsl/kafka-schemaregistry-protobuf/schema"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/briandowns/spinner"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type SseBroker struct {

	// Events are pushed to this channel from kafka consumer
	messages chan []byte

	// New client connections
	newClients chan chan []byte

	// Closed client connections
	closingClients chan chan []byte

	// Client connections registry
	clients map[chan []byte]bool
}

func newSseServer() (sseBroker *SseBroker) {
	// Instantiate an sseBroker
	sseBroker = &SseBroker{
		messages:       make(chan []byte, 1),
		newClients:     make(chan chan []byte),
		closingClients: make(chan chan []byte),
		clients:        make(map[chan []byte]bool),
	}

	// Launch the SSE server in a goroutine
	go sseBroker.listen()

	return
}

func (sseBroker *SseBroker) ServeHTTP(rw http.ResponseWriter, req *http.Request) {

	// Make sure that the writer supports flushing
	flusher, ok := rw.(http.Flusher)

	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	// Each connection registers its own message channel with the SseBroker's connections registry
	messageChan := make(chan []byte)

	// Signal the sseBroker that we have a new connection
	sseBroker.newClients <- messageChan

	// Remove this client from the map of connected clients
	// when this handler exits.
	defer func() {
		fmt.Println("closing ssebroker.closingclients")
		sseBroker.closingClients <- messageChan
	}()

	// Listen to connection close and un-register messageChan
	notify := req.Context().Done()

	go func() {
		<-notify
		sseBroker.closingClients <- messageChan
	}()

	for {
		// Write to the ResponseWriter in Server Sent Events compatible format
		fmt.Fprintf(rw, "data: %s\n\n", <-messageChan)

		// Flush the data immediatly instead of buffering it for later.
		flusher.Flush()
	}

}

func (sseBroker *SseBroker) listen() {

	for {
		select {
		case s := <-sseBroker.newClients:

			// A new client has connected.
			// Register their message channel

			sseBroker.clients[s] = true
			log.Printf("Client added. %d registered clients", len(sseBroker.clients))

		case s := <-sseBroker.closingClients:

			// A client has dettached and we want to stop sending them messages.

			delete(sseBroker.clients, s)
			log.Printf("Removed client. %d registered clients", len(sseBroker.clients))

		case event := <-sseBroker.messages:

			// New message received on kafka topic, broadcast it to all connected clients

			for clientMessageChan := range sseBroker.clients {
				clientMessageChan <- event
			}
		}
	}

}

func init() {
	rootCmd.Flags().StringP("broker", "b", "", "Kafka broker address")
	rootCmd.Flags().StringArrayP("topics", "t", []string{}, "Kafka topics to subscribe to")
	rootCmd.Flags().StringP("consumergroup", "c", "group1", "Kafka consumer group")
	rootCmd.Flags().StringP("offset", "o", "latest", "Kafka consumer offset")
	rootCmd.MarkFlagRequired("broker")
	rootCmd.MarkFlagRequired("topics")
}

var rootCmd = &cobra.Command{
	Use:   "kafka-sse-stream",
	Short: "Stream Kafka messages to SSE clients",
	Long: `Stream Kafka messages to SSE clients

	Examples:
	  kafka-sse-stream --broker 172.10.10.5 --topics topic1
	  kafka-sse-stream --broker 172.10.10.5 --topics topic1 --consumergroup group1 --offset earliest`,
	Run: func(cmd *cobra.Command, args []string) {

		kafkaBroker, _ := cmd.Flags().GetString("broker")
		kafkaTopics, _ := cmd.Flags().GetStringArray("topics")
		consumerGroup, _ := cmd.Flags().GetString("consumergroup")
		offset, _ := cmd.Flags().GetString("offset")

		sseBroker := newSseServer()

		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":     kafkaBroker,
			"broker.address.family": "v4",
			"group.id":              consumerGroup,
			"session.timeout.ms":    6000,
			"auto.offset.reset":     offset,
		})

		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create kafka consumer: %s\n", err)
			os.Exit(1)
		} else {
			fmt.Println("* Creating kafka consumer")
		}

		c.SubscribeTopics(kafkaTopics, nil)
		schemaRegistryClient := srclient.CreateSchemaRegistryClient("https://psrc-3w372.australia-southeast1.gcp.confluent.cloud")
		schemaRegistryClient.SetCredentials("", "")
		protobufResolver := lib.NewSchemaRegistryProtobufResolver(*schemaRegistryClient, protoregistry.GlobalTypes, lib.ValueDeserialization)
		deserializer := lib.NewProtobufDeserializer(protobufResolver)

		defer func() {
			s := spinner.New(spinner.CharSets[4], 100*time.Millisecond)
			s.Suffix = "  Closing kafka consumer"
			s.Start()
			c.Close()
			s.Stop()
		}()

		fmt.Println("* Starting HTTP Listener at http://localhost:3000")
		go func() {
			log.Fatal("HTTP server error: ", http.ListenAndServe(":3000", sseBroker))
		}()

		run := true

		// create a channel to listen for os signals, and close the http listener and kafka consumer when received
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			for run {
				select {
				// terminate listening on kafka bus when siganl is received
				case sig := <-sigchan:
					fmt.Printf("Caught signal %v: terminating\n", sig)
					run = false

				// poll for new messages from kafka bus
				default:
					ev := c.Poll(100)
					if ev == nil {
						continue
					}

					// push message to sseBroker channel
					switch e := ev.(type) {
					case *kafka.Message:
						value, err := deserializer.Deserialize(&kafkaTopics[0], e.Value)
						if err != nil {
							panic(fmt.Sprintf("Error consuming the message: %v (%v)", err, value))
						}

						switch v := value.(type) {
						case *schema.Orders:
							sseBroker.messages <- []byte(v.String())
							log.Printf("Broadcast message to %d registered clients", len(sseBroker.clients))
							if e.Headers != nil {
								fmt.Printf("%% Headers: %v\n", e.Headers)
							}
						default:
							fmt.Printf("unrecognized message type: %T", v)
						}
					case kafka.Error:
						// terminate if all brokers are down
						fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
						if e.Code() == kafka.ErrAllBrokersDown {
							fmt.Println("All brokers are down, terminating ...")
							run = false
						}
					default:
						// fmt.Printf("Ignored %v\n", e)
					}
				}
			}
		}()

		// Exit main routine when SIGINT, SIGTERM are caught
		<-sigchan

	},
}

func Execute() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	cobra.CheckErr(rootCmd.Execute())
}

func main() {

	Execute()

}
