package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/segmentio/kafka-go"
)

const (
	topic          = "message-log"
	broker1Address = "localhost:9092"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

func produce(ctx context.Context, message chan string, wg *sync.WaitGroup) {

	// initialize a counter
	str := <-message
	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address},
		Topic:   topic,
	})

	err := w.WriteMessages(ctx, kafka.Message{
		Key: []byte(""),
		// create an arbitrary message payload for the value
		Value: []byte("" + str),
	})
	if err != nil {
		panic("could not write message " + err.Error())
	}

	// log a confirmation once the message is written
	fmt.Println("Mesaj Teslim Edildi:", str)

	wg.Done()
	// sleep for a second
	//time.Sleep(time.Second)

}

func mesajAl(messageChannel chan string, wg *sync.WaitGroup) {

	fmt.Printf("Mesaj Bırakın : ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	messageChannel <- scanner.Text()
	wg.Done()
}

func main() {

	var wg sync.WaitGroup
	wg.Add(2)

	message2 := make(chan string)

	ctx := context.Background()

	go mesajAl(message2, &wg)
	go produce(ctx, message2, &wg)
	wg.Wait()
}
