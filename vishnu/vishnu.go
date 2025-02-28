package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	// "slices"
	"strings"
	"text/template"

	de "github.com/accuknox/dev2/api/grpc/v2/summary"
	"google.golang.org/protobuf/proto"

	// dep "github.com/accuknox/dev2/api/grpc/v1/policy"
	"github.com/accuknox/kmux"
	"github.com/accuknox/kmux/config"
	"github.com/accuknox/kmux/stream"

	// amqp "github.com/rabbitmq/amqp091-go"
	// "google.golang.org/protobuf/proto"
	"k8s.io/klog"
)

type Config struct {
	ExchangeName *string
	ExchangeType *string
	QueueName    *string
	Topic        *string
	TLSEnabled   *bool
	Server       *string
	User         *string
	Password     *string
	Filters      []string
	watch        *string
	CertFile     *string
}

func (c *Config) Print() {

	klog.Infof("Final config: %v", c.String())

}

func (c Config) String() string {
	return fmt.Sprintf(
		"ExchangeName: %v, ExchangeType: %v, QueueName: %v, Topic: %v, TLSEnabled: %v, Server: %v, User: %v, Password: %v, Watch: %v,  Filters: %v, CertFile: %v",
		deref(c.ExchangeName),
		deref(c.ExchangeType),
		deref(c.QueueName),
		deref(c.Topic),
		derefBool(c.TLSEnabled),
		deref(c.Server),
		deref(c.User),
		deref(c.Password),
		deref(c.watch),
		c.Filters,
		deref(c.CertFile),
	)
}

func deref(p *string) string {
	if p != nil {
		return *p
	}
	return "nil"
}

func derefBool(p *bool) string {
	if p != nil {
		return fmt.Sprintf("%t", *p)
	}
	return "nil"
}

func main() {

	cfg := Config{}

	cfg.Topic = flag.String("topic", "", "topic name")
	cfg.QueueName = flag.String("queue", "", "queue name")
	cfg.ExchangeName = flag.String("exchange", "", "exchange name")
	cfg.ExchangeType = flag.String("exchange-type", "direct", "exchange type")
	cfg.TLSEnabled = flag.Bool("tls", false, "enable tls")
	cfg.Server = flag.String("server", "localhost:5672", "rabbitmq server")
	cfg.User = flag.String("user", "", "rabbitmq user")
	cfg.Password = flag.String("password", "", "rabbitmq password")
	cfg.watch = flag.String("watch", "normal", "watch")

	filters := flag.String("filter", "", "comma separated list of filters")

	cfg.CertFile = flag.String("cert-file", "", "cert file")

	flag.Parse()

	cfg.Filters = strings.Split(*filters, ",")

	if *cfg.Topic == "" || *cfg.QueueName == "" || *cfg.ExchangeName == "" {
		klog.Fatal("invalid parameters")
	}

	cfg.Print()

	tmpl, err := template.ParseFiles("kmux.yaml")
	if err != nil {
		klog.Fatalf("Error parsing template: %v", err)
	}

	outFile := fmt.Sprintf("%s.yaml", *cfg.QueueName)

	// Create or overwrite the output file
	file, err := os.Create(outFile)
	if err != nil {
		klog.Fatalf("Error creating output file: %v", err)
	}
	defer file.Close()

	// Execute the template, using the string directly as the data
	if err := tmpl.Execute(file, cfg); err != nil {
		klog.Fatalf("Error executing template: %v", err)
	}

	if err := kmux.Init(&config.Options{
		LocalConfigFile: outFile,
	}); err != nil {
		klog.Fatalf("error initializing kmux: %s", err.Error())
	}

	source, err := kmux.NewStreamSource(*cfg.Topic)
	if err != nil {
		klog.Fatalf("error creating stream sink: %s", err.Error())
	}

	if err := source.Connect(); err != nil {
		klog.Fatalf("error connecting to stream source: %s", err.Error())

	}

	sumFile, err := os.OpenFile("output.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if *cfg.watch == "normal" {
		receiveData(source)
	} else if *cfg.watch == "summary" {
		// receiveSummary(source, cfg.Filters)
		receiveSummary(source, sumFile)
	} else if *cfg.watch == "delivery" {
		// test(source)
	} else if *cfg.watch == "policy" {
		receivePolicy(source, cfg.Filters)
	}

	// test(source)

	// receiveData(source)
}

// func getDelivery(source stream.Source) (<-chan amqp.Delivery, error) {

// 	rs, err := source.GetDelivery()
// 	if err != nil {
// 		return nil, err
// 	}
// 	receiver, ok := rs.(<-chan amqp.Delivery)
// 	if !ok {
// 		return nil, err
// 	}
// 	return receiver, nil
// }

// func test(source stream.Source) {
// 	receiver, err := getDelivery(source)
// 	if err != nil {
// 		klog.Fatalf("error getting delivery: %s", err.Error())
// 	}

// 	for {

// 		if receiver == nil {
// 			klog.Error("receiver is nil")
// 			continue
// 		}
// 		if source.MessagesCount() < 0 {
// 			if err = source.Reconnect(); err != nil {
// 				klog.Errorf("error reconnecting to stream source: %s", err.Error())
// 				continue
// 			}

// 			receiver, err = getDelivery(source)
// 			if err != nil {
// 				klog.Errorf("error getting delivery: %s", err.Error())
// 				continue
// 			}

// 		}
// 		msg, ok := <-receiver
// 		if !ok {
// 			continue
// 		}

// 		policy := &v2.Policy{}

// 		if err = proto.Unmarshal(msg.Body, policy); err != nil {
// 			continue
// 		}

// 		klog.Infof("policy name: %s", policy.Name)

// 		if err := msg.Ack(false); err != nil {
// 			klog.Errorf("error acknowledging message: %s", err)
// 		}
// 	}

// }

func receivePolicy(source stream.Source, filters []string) {

	// 	dataChan, errChan := source.Channel(context.Background())
	// 	for {
	// 		select {
	// 		case data := <-dataChan:
	// 			if data == nil || len(data) == 0 || string(data) == "" {
	// 				continue
	// 			}

	// 			v1Policy := dep.Policy{}

	// 			if err := proto.Unmarshal(data, &v1Policy); err != nil {
	// 				klog.Errorf("error unmarshalling data: %s", err.Error())
	// 				continue
	// 			}

	//			if len(filters) > 0 {
	//				if ok := slices.ContainsFunc(filters, func(s string) bool {
	//					name := v1Policy.GetName()
	//					if name != "" {
	//						return strings.Contains(name, s)
	//					}
	//					return false
	//				}); !ok {
	//					continue
	//				}
	//				klog.Infof("\n%+v\n", v1Policy)
	//				continue
	//			}
	//		case err := <-errChan:
	//			fmt.Printf("err: %v\n", err)
	//			if err := source.Reconnect(); err != nil {
	//				fmt.Printf("error reconnecting: %v", err)
	//			}
	//		}
	//	}
}

// func receiveSummary(source stream.Source, filters []string) {
// 	for data := range source.Channel(context.Background()) {
// 		if data == nil || len(data) == 0 || string(data) == "" {
// 			continue
// 		}

// 		v2Summary := de.SummaryEvent{}

// 		if err := proto.Unmarshal(data, &v2Summary); err != nil {
// 			klog.Errorf("error unmarshalling data: %s", err.Error())
// 			continue
// 		}

// 		if len(filters) > 0 {
// 			if ok := slices.ContainsFunc(filters, func(s string) bool {
// 				wl := v2Summary.GetWorkload()
// 				if wl != nil {
// 					return strings.Contains(wl.Name, s)
// 				}
// 				return false
// 			}); !ok {
// 				continue
// 			}
// 			klog.Infof("\n%+v\n", v2Summary)
// 			continue
// 		}

// 		if v2Summary.Operation == "Network" {

// 			if v2Summary.Namespace == "default" {
// 				klog.Infof("\n%+v\n", v2Summary)
// 			}

//		}
//	}
// }

func receiveSummary(source stream.Source, sumFile *os.File) {
	for data := range source.Channel(context.Background()) {
		if data == nil || len(data) == 0 || string(data) == "" {
			continue
		}

		v2Summary := de.SummaryEvent{}

		if err := proto.Unmarshal(data, &v2Summary); err != nil {
			klog.Errorf("error unmarshalling data: %s", err.Error())
			continue
		}

		sumByte, err := json.Marshal(&v2Summary)
		if err != nil {
			klog.Errorf("error marshalling data: %s", err.Error())
			continue
		}
		if _, err := sumFile.Write(append(sumByte, '\n')); err != nil {
			klog.Errorf("error writing data: %s", err.Error())
			continue
		}

	}
}

func receiveData(source stream.Source) {

	for data := range source.Channel(context.Background()) {

		if data == nil || len(data) == 0 || string(data) == "" {
			continue
		}

	}
}

// func receiveData(source stream.Source) {

// 	dataChan, errChan := source.Channel(context.Background())
// 	for {
// 		select {
// 		case data := <-dataChan:
// 			if data == nil || len(data) == 0 || string(data) == "" {
// 				continue
// 			}
// 			klog.Infof("data: %v", string(data))
// 		case chanErr := <-errChan:
// 			fmt.Printf("err: %v\n", chanErr)
// 			if err := source.Reconnect(); err != nil {
// 				fmt.Printf("error reconnecting: %v", err)
// 				continue
// 			}
// 			dataChan, errChan = source.Channel(context.Background())
// 		}
// 	}
// }
